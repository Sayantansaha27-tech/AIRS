#!/usr/bin/env python3
"""Replay seeded incidents and score the RCA against known ground truth.

    # deterministic path only, no infrastructure needed
    python evals/run_eval.py

    # include the LLM path (needs Ollama reachable and the model pulled)
    python evals/run_eval.py --llm --model qwen2.5:7b-instruct

    python evals/run_eval.py --json results.json

WHAT THIS MEASURES, AND WHAT IT DOES NOT
----------------------------------------
Scoring free-text root-cause analysis automatically is a genuinely unsolved
problem, and nothing here pretends otherwise. This harness measures three
things that can be checked mechanically:

  concept recall     did the RCA mention the concepts a correct answer must
                     contain (for example "pool" and "connection")
  distractor avoidance  did it avoid concluding one of the wrong answers this
                     fixture was built to bait
  service accuracy   did it name the right primary service and affected set

That is a proxy. An RCA can hit every keyword and still be incoherent, and it
can be genuinely insightful in words the fixture did not anticipate. Treat the
score as a regression signal, not as a measure of quality. See
docs/06-evals.md for the full list of what is not measured.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

sys.path.insert(0, str(REPO_ROOT / "shared"))
sys.path.insert(0, str(REPO_ROOT / "services/ai-service/app"))


@dataclass
class Score:
    fixture: str
    incident_class: str
    path: str
    concept_recall: float
    conclusion_recall: float
    distractors_avoided: bool
    primary_service_correct: bool
    affected_service_recall: float
    latency_seconds: float
    confidence: float
    root_cause: str
    missing_concepts: list[str] = field(default_factory=list)
    tripped_distractors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """A pass requires the concepts to appear in the *conclusion*.

        Scoring the whole RCA text is not enough. The deterministic fallback
        quotes raw log lines verbatim in its explanation, so it scores 92% on
        full-text concept recall while its actual root_cause field says only
        "<service> shows repeated anomalous behavior". Full-text recall
        measures whether the evidence was carried; conclusion recall measures
        whether anything was concluded from it.
        """
        return self.conclusion_recall >= 1.0 and self.distractors_avoided


def load_fixtures() -> list[dict[str, Any]]:
    return [json.loads(p.read_text()) for p in sorted(FIXTURE_DIR.glob("*.json"))]


def build_context(fixture: dict[str, Any]) -> dict[str, Any]:
    """Shape a fixture the way ai-service.build_incident_context would."""
    incident = fixture["incident"]
    logs = [
        {
            "timestamp": entry["timestamp"],
            "service": incident["service"],
            "message": entry["message"],
            "level": entry.get("severity", "warning"),
        }
        for entry in incident["timeline"]
    ]
    return {
        "incident_id": fixture["id"],
        "service": incident["service"],
        "tenant_id": "default",
        "severity": incident["severity"],
        "status": "open",
        "summary": incident["summary"],
        "timeline": incident["timeline"],
        "logs": logs,
        "top_logs": logs[:15],
        "anomalies": [{"id": f"a{i}"} for i in range(len(logs))],
        "source_metadata": {},
        "recent_incidents": [],
        "active_suppressions": [],
        "topology": [
            {"upstream": incident["service"], "downstream": name, "dependency_type": "sync"}
            for name in incident.get("related_services", [])
        ],
    }


def rca_text(rca: Any) -> str:
    parts = [
        getattr(rca, "root_cause", "") or "",
        getattr(rca, "explanation", "") or "",
        getattr(rca, "suggested_fix", "") or "",
    ]
    return " ".join(parts).lower()


def score_rca(fixture: dict[str, Any], rca: Any, path: str, elapsed: float) -> Score:
    truth = fixture["ground_truth"]
    text = rca_text(rca)

    required = [c.lower() for c in truth["must_mention"]]
    missing = [c for c in required if c not in text]
    recall = (len(required) - len(missing)) / len(required) if required else 1.0

    # Score the conclusion separately from the quoted evidence. An RCA that
    # only replays log lines carries the keywords without concluding anything.
    conclusion = (getattr(rca, "root_cause", "") or "").lower()
    concluded = [c for c in required if c in conclusion]
    conclusion_recall = len(concluded) / len(required) if required else 1.0

    tripped = [d for d in truth["must_not_conclude"] if _concludes(text, d)]

    affected = {s.lower() for s in truth["affected_services"]}
    named = {s.lower() for s in (getattr(rca, "affected_services", []) or [])}
    affected_recall = len(affected & named) / len(affected) if affected else 1.0

    return Score(
        fixture=fixture["id"],
        incident_class=fixture["incident_class"],
        path=path,
        concept_recall=recall,
        conclusion_recall=conclusion_recall,
        distractors_avoided=not tripped,
        primary_service_correct=truth["primary_service"].lower() in text
        or truth["primary_service"].lower() in named,
        affected_service_recall=affected_recall,
        latency_seconds=elapsed,
        confidence=float(getattr(rca, "confidence", 0.0)),
        root_cause=getattr(rca, "root_cause", ""),
        missing_concepts=missing,
        tripped_distractors=tripped,
    )


def _concludes(text: str, distractor: str) -> bool:
    """Crude check that the RCA asserted a known-wrong conclusion.

    Requires most of the distractor's content words to appear, so a passing
    mention of a term does not count as concluding it. This is deliberately
    blunt and will both over- and under-fire; it is documented as such.
    """
    words = [w for w in distractor.lower().split() if len(w) > 3]
    if not words:
        return False
    hits = sum(1 for w in words if w in text)
    return hits / len(words) >= 0.8


async def run_deterministic(fixtures: list[dict[str, Any]]) -> list[Score]:
    from rca import deterministic_fallback

    scores = []
    for fixture in fixtures:
        context = build_context(fixture)
        started = time.perf_counter()
        rca = deterministic_fallback(context)
        elapsed = time.perf_counter() - started
        scores.append(score_rca(fixture, rca, "deterministic", elapsed))
    return scores


async def run_llm(fixtures: list[dict[str, Any]], model: str, base_url: str) -> list[Score]:
    from ai_providers.ollama_provider import OllamaProvider
    from airs_shared.models import RCAResult
    from rca import build_prompt, deterministic_fallback

    provider = OllamaProvider(base_url=base_url, model=model, timeout_seconds=120)
    scores = []
    for fixture in fixtures:
        context = build_context(fixture)
        started = time.perf_counter()
        try:
            payload = await provider.generate(build_prompt(context), context)
            rca = RCAResult.model_validate(payload)
            path = f"llm:{model}"
        except Exception as exc:  # noqa: BLE001
            print(f"  {fixture['id']}: LLM path failed ({exc}), scoring the fallback")
            rca = deterministic_fallback(context)
            path = "deterministic (llm failed)"
        elapsed = time.perf_counter() - started
        scores.append(score_rca(fixture, rca, path, elapsed))
    return scores


def report(scores: list[Score]) -> None:
    if not scores:
        return
    path = scores[0].path
    print(f"\n{'=' * 78}\n{path}\n{'=' * 78}")
    print(
        f"{'fixture':24s} {'class':22s} {'in text':>8s} {'concluded':>10s} "
        f"{'distract':>9s} {'pass':>5s}"
    )
    print("-" * 78)
    for s in scores:
        print(
            f"{s.fixture:24s} {s.incident_class:22s} "
            f"{s.concept_recall:>7.0%} {s.conclusion_recall:>10.0%} "
            f"{'ok' if s.distractors_avoided else 'TRIPPED':>9s} "
            f"{'PASS' if s.passed else 'FAIL':>5s}"
        )
        if s.missing_concepts:
            print(f"{'':24s}   absent entirely: {', '.join(s.missing_concepts)}")
        if s.tripped_distractors:
            print(f"{'':24s}   concluded: {'; '.join(s.tripped_distractors)}")

    passed = sum(1 for s in scores if s.passed)
    print("-" * 78)
    print(f"passed                    {passed}/{len(scores)}")
    print(f"mean concept recall       {statistics.mean(s.concept_recall for s in scores):.0%}")
    print(f"mean conclusion recall    {statistics.mean(s.conclusion_recall for s in scores):.0%}")
    print(
        f"primary service correct   "
        f"{sum(1 for s in scores if s.primary_service_correct)}/{len(scores)}"
    )
    print(
        f"mean affected recall      "
        f"{statistics.mean(s.affected_service_recall for s in scores):.0%}"
    )
    print(f"median latency            {statistics.median(s.latency_seconds for s in scores):.3f}s")
    print(f"mean stated confidence    {statistics.mean(s.confidence for s in scores):.2f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llm", action="store_true", help="also score the LLM path")
    parser.add_argument("--model", default="qwen2.5:7b-instruct")
    parser.add_argument("--base-url", default="http://localhost:11434")
    parser.add_argument("--json", type=Path, help="write raw scores here")
    args = parser.parse_args()

    fixtures = load_fixtures()
    print(f"Loaded {len(fixtures)} fixtures from {FIXTURE_DIR}")

    all_scores = asyncio.run(run_deterministic(fixtures))
    report(all_scores)

    if args.llm:
        llm_scores = asyncio.run(run_llm(fixtures, args.model, args.base_url))
        report(llm_scores)
        all_scores += llm_scores

    if args.json:
        args.json.write_text(json.dumps([s.__dict__ for s in all_scores], indent=2, default=str))
        print(f"\nWrote {args.json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
