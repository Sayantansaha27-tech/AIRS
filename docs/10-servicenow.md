# 11: Choosing a model

AIRS does not care which model you use. Provider and model live entirely in
`config/airs.yaml`, and adding an endpoint that speaks the OpenAI API needs no
code at all.

---

## The short version

Change one line to switch everything:

```yaml
llm:
  provider: ollama      # or openai, anthropic, or anything you define below
```

Then restart. Config is mounted read-only into every service, so this does not
need a rebuild:

```bash
docker compose up -d
```

---

## How it fits together

Three pieces, and the middle one is the one that matters.

**Providers** are places a model can be reached. Each declares a `kind`, which
selects the adapter that talks to it, and a `models` map.

**Tiers** are roles, not model names. A provider says which of *its* models
fills each role.

**Routing** maps incident severity to a tier.

```yaml
routing:
  critical: primary        # the capable model
  warning: economy         # the cheap, fast one
  info: deterministic      # no model call at all

providers:
  ollama:
    kind: ollama
    base_url: http://ollama:11434
    models:
      primary: qwen2.5:7b-instruct
      economy: qwen2.5:1.5b-instruct
```

**Why tiers rather than model names.** An earlier design named one global model
and one global fallback model. Switching provider at runtime moved the first
and not the second, so the low-cost tier ended up asking the new provider for a
model it had never heard of. Every warning-severity RCA then burned its whole
retry budget before falling through to the deterministic path.

A tier is a role a provider fills. Switching provider moves every tier at once,
because each provider answers the question for itself.

---

## Adding a provider

### If it speaks the OpenAI chat-completions API

Most things do: vLLM, LM Studio, llama.cpp's server, Together, Groq,
OpenRouter, DeepSeek, Mistral, Anyscale, TGI, and most self-hosted gateways.
Config only, no code:

```yaml
providers:
  groq:
    kind: openai_compatible
    base_url: https://api.groq.com/openai/v1
    api_key_env: GROQ_API_KEY
    models:
      primary: llama-3.3-70b-versatile
      economy: llama-3.1-8b-instant
```

Then `provider: groq` and put `GROQ_API_KEY` in `.env`.

A local server usually needs no key at all. Omit `api_key_env` and no
`Authorization` header is sent:

```yaml
  vllm:
    kind: openai_compatible
    base_url: http://localhost:8000/v1
    models:
      primary: meta-llama/Llama-3.3-70B-Instruct
      economy: Qwen/Qwen2.5-7B-Instruct
```

### If its API is genuinely different

Anthropic is the built-in example: a top-level `system` parameter rather than a
system message, `x-api-key` rather than a bearer token, and content returned as
a list of blocks. That needs an adapter.

Write one in `services/ai-service/app/ai_providers/`, then register it:

```python
from ai_providers import register

register("my_api", lambda cfg, name, model, timeout: MyProvider(...))
```

An adapter is responsible for transport and for returning a dict. It is **not**
responsible for retries, routing, validation or fallback: those belong to the
caller, so every provider fails the same way.

---

## What the system does about bad model output

Small models wrap JSON in prose and code fences even when told not to. Throwing
away a correct answer over its packaging would be wasteful, so `coerce_json`
recovers the common shapes: a bare object, a fenced block, or an object
embedded in explanation.

What it will not do is guess. Genuinely unparseable output returns
`{"raw": ...}`, which fails `RCAResult` validation and falls through the tier
cascade. **A provider never invents a result.**

Not every OpenAI-compatible server implements `response_format`. When one
rejects it, the adapter retries the same request without it rather than failing
the tier.

---

## Choosing, in practice

| Situation | Reasonable choice |
| --- | --- |
| Laptop, no API budget, data must stay local | `ollama`, the default. Both tiers use the 1.5B, so it is one 1 GB pull |
| Same, but you have the disk | raise `primary` to `qwen2.5:7b-instruct`, a one-line change |
| Laptop with a GPU, want better quality | `ollama` with larger models in the tiers |
| Best available reasoning, cost is fine | `anthropic` or `openai` |
| Self-hosted GPU box | `openai_compatible` at your vLLM or TGI endpoint |
| Want speed above all | `openai_compatible` at Groq |
| Many models behind one key | `openai_compatible` at OpenRouter |

Two things worth knowing before you pick.

**Measured RCA latency is dominated by the model, not by AIRS.** On CPU-only
Ollama, a 1.5B model took 20 seconds per RCA. The pipeline around it delivers
an incident in 270 ms. See [06-evals.md](06-evals.md).

**The default is deliberately small.** Both tiers ship pointing at
`qwen2.5:1.5b-instruct`, so a first run needs one 1 GB download instead of
six, and works on a constrained laptop. That makes the two tiers identical
until you change one: routing still sends `info` to the deterministic path, so
the mechanism is live, but the primary/economy distinction only starts paying
once `primary` names a larger model.

**Quality is barely measured.** The eval harness covers six seeded incidents,
and the 7B tier has no results at all. A small model passed 3 of 6, and every
one it passed had the cause named literally in a log line. Do not read the
defaults here as a recommendation backed by evidence: they are a starting
point. Run `evals/run_eval.py --llm` against your own choice.

---

## What never changes

Whatever you pick, `info`-severity incidents call no model, and any incident
whose model calls all fail still gets a deterministic RCA. The pipeline does
not depend on model availability, which is asserted by the chaos suite rather
than by this sentence. See [`tests/chaos`](../tests/chaos).

---

Previous: [10: ServiceNow](10-servicenow.md) | Back to [README](../README.md)
