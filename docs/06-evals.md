# 06: Evals

Measured numbers, the method that produced them, and an explicit list of what
is not measured. Where a number is bad it is published anyway with the
constraint that caused it.

Terminology is used strictly throughout, per the writing rules:

- **Measured** means observed in a real run on the hardware stated below.
- **Benchmarked** means observed under a stated synthetic evaluation.
- **Modelled** means an estimate with assumptions, and is labelled as such.

---

## Hardware and method

Every number on this page was produced on one machine.

| | |
| --- | --- |
| Machine | Apple M3, 8 cores, 16 GB |
| OS | macOS 26.5 (Darwin arm64) |
| Runtime | Docker Desktop 29.1.3, full `docker compose` stack |
| OpenSearch | 2.15.0, single node, 512 MB heap |
| Kafka | confluentinc/cp-kafka, KRaft single node |
| Model runtime | Ollama in Docker, **CPU only** |

**The most important constraint:** Docker Desktop on macOS does not pass Metal
through to containers, so every LLM number here is CPU inference. On a host
with GPU access, or against a hosted API, RCA latency will be several times
better. Treat the model latencies as a floor on this configuration, not as a
property of the system.

Everything runs on one machine, so the load generator competes with the stack
it is measuring. That biases the throughput numbers low, and it is the same
condition a reader running the quickstart will be in.

Reproduce with:

```bash
docker compose up -d
python evals/load_test.py --rate 200 --seconds 30 --e2e-samples 5
python evals/bench_pipeline.py --events 50000
python evals/run_eval.py --llm --model qwen2.5:1.5b-instruct
```

---

## Sustained throughput, and what breaks first

Measured, 25 to 30 second runs at each rate, 10% error ratio across 8 synthetic
services.

Measured twice: before the async and bulk-indexing work, and after.

| Target rate | Logs processed (before) | Logs processed (after) | correlation-service | DLQ |
| --- | --- | --- | --- | --- |
| 200/s | 100% | 100% | reachable | 0 |
| 400/s | 88% | **100%** | blocked -> **reachable** | 0 |
| 700/s | 61% | **100%** | blocked -> **reachable** | 0 |
| 1000/s | 40% | **100%** | blocked -> **reachable** | 0 |
| 2667/s | not reached | **100%** | reachable | 0 |

After, at 1000/s: 978.6 achieved, 0 rejected, batch p99 39.6 ms.
At 2667/s: 0 rejected, batch p99 71.8 ms, tail latency degraded (see below).

**Sustained rate before lag grows: roughly 1,000 events/sec**, up from roughly
200. It still processes every event at 2,667/sec, with a degraded tail rather
than a failure.

Two things to read carefully here.

**Ingestion is not the bottleneck.** It accepted 979.7 events/sec with zero
rejections and a p99 of 41.6 ms. Publishing to Kafka is cheap. Every failure
below is a consumer failing to keep up, which is exactly the shape the
architecture predicts: backpressure does not propagate upstream, so the front
door stays open while the pipeline falls behind it.

**log-processor was the first bottleneck, and no longer is.** It used to
plateau near 10,000 events per 25 seconds regardless of load, because it
indexed every event with `refresh=True` through the synchronous client from
inside an async loop. It now normalizes a whole Kafka batch, indexes it in one
bulk request with `refresh=False`, and keeps up at every rate tested.

### What used to break first: correlation-service stopped answering

The most useful finding of the first load test, and it was not a throughput
number. It is recorded here because the diagnosis is the interesting part, and
because the fix followed directly from it.

At **400 events/sec and above, correlation-service stopped responding
entirely**. Not slow: unresponsive. `/health/live` timed out after 5 seconds
and `/metrics` returned nothing, while the process was running and had not been
OOM killed or restarted.

Its logs during the run:

```
Heartbeat failed: local member_id was not recognized; resetting and re-joining group
Heartbeat session expired - marking coordinator dead
Marking the coordinator dead (node 1) for group airs-correlation-service
OffsetCommit failed for group airs-correlation-service due to group error
  ([Error 25] UnknownMemberIdError), will rejoin
Auto offset commit failed: [Error 25] UnknownMemberIdError
```

That is a single causal chain, and it connects three failure modes that
[05-failure-modes.md](05-failure-modes.md) documents separately:

1. Every incident emission makes **synchronous** OpenSearch calls from inside
   the async consumer loop: a topology-neighbour search, a parent-incident
   search, and one or two document writes. Each blocks the event loop.
2. A blocked event loop cannot send Kafka heartbeats, so the broker evicts the
   consumer from its group.
3. Eviction forces a rebalance, and the auto-commit offset write fails against
   a group the consumer is no longer a member of.

Step 3 was the silent-data-loss path, observed happening under ordinary load
rather than only during a deploy. The blocking-I/O defect was not a throughput
inconvenience; it was what triggered the durability defect.

**Both ends are now fixed.** correlation-service uses the async OpenSearch
client, so its event loop stays free and heartbeats keep flowing; and
consumers commit manually with a durable idempotency key, so a rebalance
replays rather than loses. At 1000 events/sec correlation-service now stays
reachable throughout.

This also explains why an early run reported negative counter deltas: a
service that stops serving `/metrics` is not a service doing zero work, and
the load test now says so explicitly rather than subtracting.

### DLQ rate

| Condition | DLQ events |
| --- | --- |
| 200/s, healthy | **0** |
| 400/s, degraded | **0** |
| 700/s, degraded | **0** |
| 1000/s, broken | **0** |

Zero throughout, which is correct and easy to misread. Nothing was malformed,
so nothing was dead-lettered. Events lost to the rebalance path above **do not
appear here**, because they never failed: they were never processed. A DLQ
depth of zero is not evidence that nothing was lost.

---

## Latency

### End-to-end, log to queryable incident

Measured with a critical-severity event, which emits an incident on arrival and
therefore excludes the correlation window.

| Load | p50 | p95 | Samples |
| --- | --- | --- | --- |
| Idle | **0.28 s** | 0.30 s | 5 |
| 1000/s, before | no incident within 120 s | | 3 |
| 1000/s, after | **0.27 s** | 0.27 s | 3 |
| 2667/s, after | 0.28 s | **16.64 s** | 3 |

Under 300 ms idle, through four Kafka hops and two OpenSearch writes, is the
number the architecture was aimed at. What changed is that it now holds *under
load*: at 1000 events/sec the p50 and p95 are both 0.27 s, where previously no
incident appeared at all within a two-minute timeout.

The p95 at 2667/sec shows saturation arriving: the median is unchanged while
the tail degrades by two orders of magnitude, which is the signature of a queue
rather than a slowdown.

**A non-critical incident is bounded by correlation, not transport.** An
isolated warning waits for `window_duration_minutes` (default 10 minutes)
before an incident exists at all. No amount of pipeline tuning changes that; it
is a product decision, documented in the README latency table.

### RCA generation

| Path | Latency | Note |
| --- | --- | --- |
| Deterministic fallback, in process | **< 1 ms** | benchmarked, 20,000 iterations |
| Model reachable but model name absent | **3.1 s** | measured, fast-fail through both tiers then fallback |
| qwen2.5:1.5b-instruct | **20.1 s** median | measured, CPU only |
| Model unreachable (timeout, not refusal) | **~54 s** | modelled: 6 attempts x 8 s timeout plus 1 s + 2 s backoff, confirmed by attempt count |

The gap between rows 2 and 4 is worth understanding operationally. A model that
**refuses fast** costs 3 seconds per incident. A model that **hangs** costs 54.
The second is the outage shape that grows unbounded queue, and it is why the
runbook's load-shedding step is to point the provider at a nonexistent model
rather than at nothing.

At 20 seconds per RCA on CPU, a single sequential consumer sustains about 3
incidents per minute. That is the real ceiling on RCA throughput on this
hardware, and it is why `info` severity never calls a model at all.

---

## Chaos verification

The claim the whole design rests on is that the pipeline does not depend on
model availability. Run against the live stack:

```
AIRS_CHAOS=1 pytest tests/chaos -v

test_incident_still_gets_rca_with_ai_service_down       PASSED
test_no_duplicate_incident_after_recovery               PASSED
test_deterministic_rca_when_the_model_is_unreachable    PASSED
test_ingestion_stays_available_throughout               PASSED

4 passed in 42.76s
```

What that run actually established, with ai-service stopped mid-flight:

| Assertion | Result |
| --- | --- |
| Ingestion keeps accepting batches | 5 batches, 25 events, all accepted |
| Incidents are still created | yes, written by correlation before RCA exists |
| DLQ does not grow | unchanged |
| Recovery enriches the queued incident | yes |
| Recovery does not duplicate incidents | same ids before and after |
| Unreachable model still yields an RCA | deterministic, and it says so |

This is the difference between an architecture diagram and a property. It is
documented as a release gate in [07-runbook.md](07-runbook.md) rather than wired
into CI, because it needs the full stack and stops containers as part of the
test.

---

## Per-stage CPU cost

Benchmarked with Kafka and OpenSearch stubbed out, 50,000 synthetic events.
These are **upper bounds on the CPU-bound portion only**, and the measured
end-to-end numbers above are the ones to quote.

| Stage | Events/sec | p50 | p99 |
| --- | --- | --- | --- |
| normalize | 506,871 | 1.87 µs | 2.38 µs |
| contract validation | 549,075 | 1.75 µs | 2.29 µs |
| anomaly detection | 548,880 | 2.21 µs | 2.96 µs |
| correlation | 131,614 | 1.25 µs | **170.79 µs** |
| deterministic RCA | 134,771 | 7.33 µs | 8.79 µs |

The conclusion when this was first measured was stark: the pure computation
ran at roughly **500,000 events/sec** while the deployed pipeline managed
**200**, so nothing in the detection or correlation logic needed optimising and
the entire gap was I/O. Acting on that closed most of it: the deployed pipeline
now sustains **1,000** and processes cleanly at **2,667**. It remains I/O bound,
which is the expected state for a system whose job is to move events between a
log, a search index and a model.

Correlation's p99 is 137x its p50 because the emit path (building the timeline,
validating the model, resolving the parent) is far more expensive than the
common path of adding an anomaly to an existing cluster. That is expected and
benign.

---

## RCA quality

Full results and per-fixture outputs in [`evals/results.md`](../evals/results.md).
Six seeded incidents with known root causes, each built with a distractor.

| Path | Passed | Conclusion recall | Median latency | Mean stated confidence |
| --- | --- | --- | --- | --- |
| Deterministic fallback | **0/6** | 0% | < 1 ms | 0.70 |
| qwen2.5:1.5b-instruct | **3/6** | 58% | 20.1 s | **0.92** |

### The scoring metric was wrong first, which is the finding

The harness originally scored required concepts appearing anywhere in the RCA
text. On that metric the **deterministic fallback scored 92%**, because it
quotes raw log lines verbatim in its explanation. The keywords were present
because the evidence had been copied, not because anything had been concluded.
Its `root_cause` field says `"<service> shows repeated anomalous behavior"`.

A metric the null baseline can max out is not measuring what it claims to.
Scoring the `root_cause` field separately from the quoted evidence fixed it,
and the deterministic path correctly drops to 0%.

This is worth stating plainly because it is the most likely way an eval harness
misleads its own author: the number went up, and up was wrong.

### What the 1.5B model can and cannot do

Every fixture it passed has its root cause **named literally in a log line**:
`OutOfMemoryError`, `No space left on device`, `certificate has expired`. Every
fixture it failed requires **inferring a cause from a relationship between
lines**: pool statistics explaining the timeouts, a dependency's 503 explaining
a checkout failure, a SIGTERM plus a version change meaning a deploy.

On this evidence the small model does entity extraction, not causal reasoning.
Its worst failure named the reporting service as the cause when the fault was
in its dependency, which is precisely the mistake the 2 AM scenario in
[00-problem.md](00-problem.md) describes humans making.

### Confidence carries almost no signal

Mean stated confidence 0.92 against a 3/6 pass rate. Confidence never fell
below 0.90 on any fixture, including all three failures, and one failure was
stated at 1.00.

AIRS already blends model self-assessment with a structural heuristic rather
than trusting it. This measures how necessary that is: the model's own number
is close to uninformative, so the blend is dampening noise rather than
combining two estimates. **`confidence` should not be shown to an operator as a
probability.**

---

## What is not measured

This list is longer than the measured list. That is the honest state of it.

**RCA quality**

- **Accuracy on real incidents.** Six synthetic fixtures with answers written
  by the same person who wrote the prompts is not a corpus. This is the single
  biggest gap and nothing else on this page compensates for it.
- **Whether an operator finds the RCA useful.** The feedback endpoint collects
  ratings and nothing reads them. That was meant to answer this.
- **Whether the RCA is coherent.** Concept recall cannot tell a well-reasoned
  answer from a keyword-dense one.
- **Whether an RCA is misleading rather than merely wrong.** A confidently
  wrong cause at 2 AM is worse than no answer, and this harness scores both as
  a single FAIL.
- **The 7B tier, at all.** `qwen2.5:7b-instruct` is what `critical` incidents
  route to and it has no results. The only run was invalidated by the model
  container being stopped part-way through, and the re-run did not fit on the
  measuring machine's disk. So the tier that matters most is the one with no
  quality data, and severity routing is currently justified by cost and latency
  rather than by measured quality. Reproduction steps in
  [`evals/results.md`](../evals/results.md).

**Detection and correlation**

- **False positive and false negative rates.** No labelled stream of "this was
  really an anomaly" exists, so the anomaly threshold is unvalidated.
- **Whether correlation groups what a human would group.** No ground truth for
  "these five anomalies are one incident". The eval harness scores RCA text,
  not grouping.
- **Whether the seasonal baseline earns its complexity.** Untested against real
  seasonal traffic, and largely bypassed by the emission disjunction anyway.
  Under load test its own generator looks anomalous, since the baseline tracks
  volume and a load test is by definition a volume anomaly.
- **Fingerprint collision and over-specificity rates** on real log formats.

**Operational**

- **Behaviour beyond 30 seconds of load.** All runs are short. Nothing here
  says what happens after an hour, when OpenSearch segment merging and Kafka
  retention start to matter.
- **Multi-node anything.** Single-node Kafka, single-node OpenSearch, one
  replica per service. Rebalance behaviour with real partition counts is
  untested.
- **Recovery time** after the correlation-service stall, other than that a
  restart clears it.
- **Memory behaviour over time.** In-memory baselines and clusters grow with
  service count and were never watched over a long run.
- **Anything on GPU.** Every model number is CPU-only.

---

Previous: [05: Failure modes](05-failure-modes.md) | Next: [07: Runbook](07-runbook.md)
