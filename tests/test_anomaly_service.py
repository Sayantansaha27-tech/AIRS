"""anomaly-service: detection contract, baseline behaviour, emission gates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from airs_shared.models import DetectionRule, RuleMatchType, Severity, SuppressionWindow

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ----------------------------------------------------------------- contract


def test_fingerprint_is_stable_and_scoped(anomaly):
    a = anomaly.build_fingerprint("default", "orders-service", "timeout")
    b = anomaly.build_fingerprint("default", "orders-service", "  TIMEOUT  ")
    assert a == b, "fingerprint is case and whitespace insensitive"

    assert a != anomaly.build_fingerprint("other", "orders-service", "timeout")
    assert a != anomaly.build_fingerprint("default", "payments-service", "timeout")
    assert len(a) == 16


def test_fingerprint_collapses_embedded_ids(anomaly):
    """The same failure with a different request id is one signal, not two.

    Without templatizing, deduplication silently stops working for any service
    that embeds ids in its messages.
    """
    a = anomaly.build_fingerprint("default", "s", "timeout req_id=8a3f2b1c9d0e")
    b = anomaly.build_fingerprint("default", "s", "timeout req_id=9b2c7f4a1e3d")
    assert a == b


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("pool exhausted after 1204ms", "pool exhausted after 87ms"),
        ("conn from 10.0.1.5:5432 refused", "conn from 10.0.2.9:5432 refused"),
        ("failed at 2026-08-11T02:14:03Z", "failed at 2026-08-11T09:41:55Z"),
        (
            "job 3f2b8a1c-4d5e-6f70-8912-a3b4c5d6e7f8 failed",
            "job 9c8d7e6f-5a4b-3c2d-1e0f-9a8b7c6d5e4f failed",
        ),
        ("retry 3 of 5", "retry 4 of 5"),
        ("segment 0x1f4a corrupt", "segment 0xbeef corrupt"),
    ],
)
def test_varying_values_do_not_split_a_fingerprint(anomaly, first, second):
    assert anomaly.build_fingerprint("t", "s", first) == anomaly.build_fingerprint("t", "s", second)


def test_genuinely_different_messages_still_differ(anomaly):
    """Templatizing must not collapse unrelated failures into one."""
    a = anomaly.build_fingerprint("t", "s", "connection pool exhausted")
    b = anomaly.build_fingerprint("t", "s", "certificate has expired")
    assert a != b


def test_templatize_masks_only_the_varying_parts(anomaly):
    assert anomaly.templatize("timeout after 1204ms on 10.0.0.1") == ("timeout after <qty> on <ip>")


# ------------------------------------------------------------ emission gates


@pytest.mark.parametrize(
    ("level", "keywords", "expected"),
    [
        ("info", [], False),
        ("error", [], True),
        ("critical", [], True),
        ("fatal", [], True),
        ("info", ["timeout"], True),
        ("debug", ["oom"], True),
    ],
)
def test_emission_is_a_disjunction_not_a_threshold(anomaly, level, keywords, expected):
    """z-score of 0.0 is below anomaly_threshold, so any emission here is
    driven by the keyword or level gate rather than the threshold."""
    assert anomaly.should_emit(0.0, level, keywords, []) is expected


def test_zscore_alone_can_emit(anomaly):
    threshold = anomaly.settings.pipeline.anomaly_threshold
    assert anomaly.should_emit(threshold + 0.1, "info", [], []) is True
    assert anomaly.should_emit(threshold - 0.1, "info", [], []) is False


def test_a_matching_rule_always_emits(anomaly):
    rule = DetectionRule(name="r", pattern="x", service_pattern="*")
    assert anomaly.should_emit(0.0, "info", [], [rule]) is True


# ------------------------------------------------------------------ severity


@pytest.mark.parametrize(
    ("level", "score", "keywords", "expected"),
    [
        ("info", 0.0, [], Severity.info),
        ("error", 0.0, [], Severity.warning),
        ("critical", 0.0, [], Severity.critical),
        ("info", 4.5, [], Severity.critical),
        ("info", 0.0, ["oom"], Severity.critical),
        ("info", 0.0, ["connection refused"], Severity.critical),
        ("info", 0.0, ["timeout"], Severity.info),
    ],
)
def test_severity_derivation(anomaly, level, score, keywords, expected):
    assert anomaly.derive_severity(level, score, keywords) == expected


def test_rules_can_raise_but_never_lower_severity(anomaly):
    critical_rule = DetectionRule(name="r", pattern="x", severity=Severity.critical)
    info_rule = DetectionRule(name="r", pattern="x", severity=Severity.info)

    assert anomaly.combine_severity(Severity.info, [critical_rule]) == Severity.critical
    assert anomaly.combine_severity(Severity.critical, [info_rule]) == Severity.critical


# ------------------------------------------------------------------ baseline


def test_baseline_is_per_tenant_and_service(anomaly):
    a = anomaly.get_baseline("default", "orders-service")
    b = anomaly.get_baseline("default", "payments-service")
    c = anomaly.get_baseline("other", "orders-service")
    assert a is not b and a is not c
    assert a is anomaly.get_baseline("default", "orders-service")


def test_baseline_slot_is_cold_until_that_hour_has_been_seen(anomaly):
    baseline = anomaly.ServiceSeasonalBaseline(alpha=anomaly.EWMA_ALPHA)
    t = datetime(2026, 8, 10, 3, 0, tzinfo=UTC)
    baseline.ingest(t)
    assert baseline.zscore(t + timedelta(hours=5)) is None, "unseen hour-of-day slot"


def test_baseline_tracks_volume_not_error_rate(anomaly):
    """Documented limitation: the baseline counts every event, so a burst of
    info-level traffic is anomalous."""
    baseline = anomaly.ServiceSeasonalBaseline(alpha=anomaly.EWMA_ALPHA)
    t = datetime(2026, 8, 10, 3, 0, tzinfo=UTC)
    for minute in range(10):
        for _ in range(5):
            baseline.ingest(t + timedelta(minutes=minute))

    burst = t + timedelta(minutes=11)
    for _ in range(100):
        baseline.ingest(burst)

    assert baseline.zscore(burst) > anomaly.settings.pipeline.anomaly_threshold


def test_anomaly_score_never_goes_negative(anomaly):
    baseline = anomaly.ServiceSeasonalBaseline(alpha=anomaly.EWMA_ALPHA)
    t = datetime(2026, 8, 10, 3, 0, tzinfo=UTC)
    for minute in range(5):
        for _ in range(50):
            baseline.ingest(t + timedelta(minutes=minute))
    quiet = t + timedelta(minutes=6)
    baseline.ingest(quiet)
    assert baseline.zscore(quiet) < 0, "a quiet minute scores below the mean"
    assert max(round(baseline.zscore(quiet), 3), 0.0) == 0.0


# --------------------------------------------------------------- suppression


def test_suppression_matches_glob_within_the_window(anomaly):
    now = datetime.now(UTC)
    anomaly.suppressions_cache = [
        SuppressionWindow(
            service_pattern="payments-*",
            reason="planned maintenance",
            starts_at=now - timedelta(minutes=5),
            ends_at=now + timedelta(minutes=5),
        )
    ]

    assert anomaly.get_matching_suppression("default", "payments-api", now) is not None
    assert anomaly.get_matching_suppression("default", "orders-service", now) is None


def test_suppression_respects_window_bounds_and_tenant(anomaly):
    now = datetime.now(UTC)
    anomaly.suppressions_cache = [
        SuppressionWindow(
            service_pattern="*",
            reason="maintenance",
            starts_at=now - timedelta(minutes=10),
            ends_at=now - timedelta(minutes=5),
            tenant_id="team-a",
        )
    ]
    assert anomaly.get_matching_suppression("team-a", "any", now) is None, "expired"
    assert anomaly.get_matching_suppression("team-b", "any", now - timedelta(minutes=7)) is None, (
        "wrong tenant"
    )
    assert anomaly.get_matching_suppression("team-a", "any", now - timedelta(minutes=7)) is not None


# --------------------------------------------------------------------- rules


@pytest.mark.parametrize(
    ("match_type", "pattern", "message", "expected"),
    [
        (RuleMatchType.keyword, "OOM", "process hit oom killer", True),
        (RuleMatchType.keyword, "OOM", "everything is fine", False),
        (RuleMatchType.regex, r"OOM|OutOfMemory", "java.lang.OutOfMemoryError", True),
        (RuleMatchType.regex, "[invalid(regex", "anything", False),
        (RuleMatchType.composite, "pool && exhausted", "db pool is exhausted", True),
        (RuleMatchType.composite, "pool && exhausted", "db pool is fine", False),
    ],
)
def test_rule_matching(anomaly, match_type, pattern, message, expected):
    rule = DetectionRule(name="r", match_type=match_type, pattern=pattern)
    assert (
        anomaly.matches_rule(rule=rule, service="any-service", message=message, score=None)
        is expected
    )


def test_threshold_rule_needs_a_score(anomaly):
    rule = DetectionRule(name="r", match_type=RuleMatchType.threshold, pattern="3.0")
    assert anomaly.matches_rule(rule=rule, service="s", message="m", score=None) is False
    assert anomaly.matches_rule(rule=rule, service="s", message="m", score=3.5) is True
    assert anomaly.matches_rule(rule=rule, service="s", message="m", score=2.0) is False


def test_service_pattern_scopes_the_rule(anomaly):
    rule = DetectionRule(name="r", pattern="x", service_pattern="orders-*")
    assert anomaly.matches_rule(rule=rule, service="orders-api", message="x", score=None)
    assert not anomaly.matches_rule(rule=rule, service="payments", message="x", score=None)


# ---------------------------------------------------------------- confidence


def test_confidence_rises_with_corroborating_signal(anomaly):
    weak = anomaly.compute_confidence_score(
        level="info", score=0.0, matched_keywords=[], matched_rules=[]
    )
    strong = anomaly.compute_confidence_score(
        level="critical",
        score=6.0,
        matched_keywords=["oom", "timeout"],
        matched_rules=[DetectionRule(name="r", pattern="x", confidence_boost=0.4)],
    )
    assert weak < 0.3, "a bare info event is below the emission floor"
    assert strong > weak
    assert 0.0 <= strong <= 1.0


# ----------------------------------------------------------------- DLQ path


async def test_undecodable_anomaly_candidate_routes_to_dlq(anomaly, producer):
    await anomaly.publish_to_dlq(
        payload={"service": "s", "timestamp": "NOT-A-TIMESTAMP"},
        error=ValueError("Invalid isoformat string"),
        partition=2,
        offset=99,
    )

    dlq = producer.payloads_for(anomaly.settings.kafka.topics.dlq)
    assert len(dlq) == 1
    assert dlq[0]["source_topic"] == anomaly.settings.kafka.topics.processed_logs
    assert dlq[0]["original_partition"] == 2
    assert dlq[0]["original_offset"] == 99
