# 09: Postmortem

What I would do differently, written after auditing the system against its own
documentation and finding that the documentation was wrong in seven places.

---

## The audit is the finding

The most useful hour spent on AIRS was not writing code. It was reading the
README next to the implementation and checking every architectural claim.

Seven claims did not hold. Two were wrong in ways that mattered:

- **Fingerprint deduplication stopped working under load.** Clusters were
  discarded on emit, taking their fingerprint set with them, so 50 identical
  critical anomalies produced 50 incidents and 50 RCA generations. The feature
  worked in every case except the one it exists for.
- **The deterministic fallback did not cover provider misconfiguration.**
  `build_provider()` sat outside the guarded block, so selecting OpenAI without
  an API key raised, dead-lettered the incident, and left it with `rca: null`
  permanently. The headline promise, that the pipeline never depends on model
  availability, failed on the most likely way it breaks in practice.

The rest were documentation drift: an anomaly algorithm described as a
15-minute error-rate window when it was a per-hour-of-day EWMA over total
volume, a correlation algorithm describing work that happens in a different
service, a Redis cache that does not exist, a feedback loop that only collects,
and two of three usage examples that returned 422 as printed.

**None of this was discoverable by running the system.** It all worked. It just
did not work the way it said it did.

### What I would do differently

Write the claim and the test that proves it in the same commit. Every one of
these would have been caught by a test that asserted the documented behaviour
rather than the implemented behaviour. The chaos suite and the 131 service
tests exist now, but they were written after the fact, which is the wrong
order.

---

## The design decisions I would revisit

### 1. Auto-commit was the wrong default, and I would fix it first

Every consumer uses `enable_auto_commit=True`. Offsets advance on a timer
regardless of whether processing succeeded, so a hard kill mid-batch loses
in-flight records permanently. They do not appear in the DLQ, because they
never failed.

This is the only failure mode in the system that silently loses data, and it
was chosen by not choosing: auto-commit is the default, and defaults are
decisions you did not notice making.

What makes it more than a one-line fix is the second half. Manual commits give
at-least-once delivery, which requires idempotent handlers. Indexing is already
idempotent by document id. **Incident creation is not:** replaying an anomaly
opens a second incident. Doing this properly means the deduplication
fingerprint becoming a durable idempotency key rather than an in-memory set,
which is the same change as moving correlation state out of process.

I would do the two together, and I would do them before anything else on the
backlog.

### 2. Correlation state in memory bought less than it cost

It is fast and it needs no coordination, which is what I optimised for. What I
underweighted is how much it constrains: correlation-service cannot scale
horizontally, loses open clusters on an unclean exit, and cannot make the
deduplication fingerprint durable, which is what blocks the fix above.

Redis was already in the stack, gated on at startup and health-checked, and
used for nothing. Putting cluster state there from the start would have cost
one round trip per anomaly and removed three separate constraints.

I would still not put it in OpenSearch. That was the right rejection.

### 3. Emission being a disjunction was never a decision

`should_emit` fires on any rule, any keyword, an error-level log, or a z-score
above threshold. The seasonal baseline is the sophisticated part of the system
and it is mostly bypassed: in normal operation the keyword and level gates fire
far more often than the threshold does.

This is defensible during cold start, when no hour-of-day slot is populated. It
is not defensible permanently, and it means the anomaly detection is closer to
`grep -i error` than the architecture suggests.

I would gate on the baseline once it is warm and keep the keyword path only as
a cold-start bridge, then measure how much the baseline is actually
contributing. Right now I cannot tell you, which is itself the answer.

### 4. Synchronous OpenSearch inside async loops

Every service builds the blocking client and calls it from async code. Every
call stalls the event loop. `GET /v1/stream` runs a blocking search every two
seconds per connected client on the gateway's single loop, and log-processor
indexes with `refresh=True` on every event, forcing a refresh per document.

This one is embarrassing rather than subtle. The async client exists, bulk
indexing exists, and `refresh=False` with an interval is the normal
configuration. I built the pipeline shape first and never revisited the I/O,
which is a familiar way to end up with an architecture that is correct and a
throughput that is not.

---

## What I got right and would keep

**Incidents exist before RCA does.** correlation-service writes and publishes a
complete incident before ai-service ever sees it. Every good property under
model failure follows from that one ordering decision, and it cost nothing.

**Validation at the produce boundary.** `produce_json` validates against the
registered model before publishing, so a malformed event cannot reach a topic
and consumers deserialize without defensive parsing. Cheap, and it eliminated
an entire class of bug.

**Severity routing.** The tiers genuinely differ and `info` never calls a model
at all. This turned out to matter more than expected once measured: on the
hardware in [06-evals.md](06-evals.md), inference is 15 to 25 seconds, so not
calling a model is the single largest latency win available.

**Splitting the RCA success metric by path.** Adding `path="deterministic"`
converted an invisible degradation into a visible one. Before that, a total
model outage produced a flawless success rate.

---

## What I still cannot tell you

The uncomfortable list, and it is longer than the measured one.

- **Whether the RCA is right on real incidents.** Six synthetic fixtures with
  known answers is not a corpus. I have never run this against a real incident
  stream with a real operator judging the output.
- **Whether correlation groups what a human would group.** There is no ground
  truth for "these five anomalies are one incident". The eval harness scores
  RCA text, not grouping.
- **Whether the seasonal baseline earns its complexity.** Untested against real
  seasonal traffic, and largely bypassed by the emission disjunction anyway.
- **What throughput this sustains end to end.** The per-stage numbers are
  CPU-bound upper bounds with Kafka and OpenSearch stubbed. The deployed system
  is I/O bound somewhere below them and I have not measured where.
- **Whether anyone finds the RCA useful.** The feedback endpoint exists and
  collects ratings. Nothing reads them. That was going to be the answer to this
  question and it is still just a table.

---

## The one-sentence version

The architecture was sound and the claims about it were not, which is a more
common failure than a broken architecture and a much harder one to notice from
inside.

---

Previous: [08: Handoff](08-handoff.md) | Back to [README](../README.md)
