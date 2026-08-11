"""ServiceNow source and sink.

Lives here rather than in `shared/` deliberately: ADR-007 requires that no core
pipeline service knows ServiceNow exists, and anything in `shared/` is on every
service's import path. Only connector-service imports this.

Two halves:

- `ServiceNowSource` pulls incidents from the Table API and maps them onto the
  `Incident` contract, entering the pipeline at `incidents-topic` because a
  ServiceNow incident is already an incident.
- `ServiceNowWorkNoteSink` writes the RCA back as a work note on the
  originating ticket, through `OutboundAdapter` so retries cannot double-post.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import httpx
from airs_shared.models import Incident, Severity
from airs_shared.sinks import OutboundAdapter, Sink, SinkPayload, SinkResult
from airs_shared.sources import Source, SourceCursor, SourceKind, SourceRecord

logger = logging.getLogger("connector.servicenow")

SOURCE_SYSTEM = "servicenow"

# ServiceNow priority is 1 (Critical) through 5 (Planning).
PRIORITY_TO_SEVERITY = {
    "1": Severity.critical,
    "2": Severity.critical,
    "3": Severity.warning,
    "4": Severity.info,
    "5": Severity.info,
}

INCIDENT_FIELDS = ",".join(
    [
        "sys_id",
        "number",
        "short_description",
        "description",
        "priority",
        "urgency",
        "impact",
        "state",
        "opened_at",
        "sys_updated_on",
        "sys_updated_by",
        "cmdb_ci",
        "assignment_group",
        "category",
    ]
)


def _display(value: Any) -> str:
    """ServiceNow returns references as {"value": ..., "display_value": ...}."""
    if isinstance(value, dict):
        return str(value.get("display_value") or value.get("value") or "").strip()
    return str(value or "").strip()


def _parse_timestamp(raw: str) -> datetime:
    """ServiceNow emits 'YYYY-MM-DD HH:MM:SS' in the instance's timezone.

    Treated as UTC. A PDI defaults to UTC, and guessing at an offset we were
    not told is worse than being consistently wrong in one direction.
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw.strip(), fmt).replace(tzinfo=UTC)
        except (ValueError, AttributeError):
            continue
    return datetime.now(UTC)


def derive_service(record: dict[str, Any]) -> str:
    """Best available name for what broke.

    Preference order matches how much the field actually identifies a system:
    the configuration item is the thing, the assignment group is who owns it,
    the category is a last resort.
    """
    for key in ("cmdb_ci", "assignment_group", "category"):
        value = _display(record.get(key))
        if value:
            return value
    return "unknown-service"


def derive_severity(record: dict[str, Any]) -> Severity:
    priority = _display(record.get("priority"))
    # Reference fields can arrive display-valued ("1 - Critical"), so take the
    # leading digit rather than requiring an exact match.
    if priority and priority[0] in PRIORITY_TO_SEVERITY:
        return PRIORITY_TO_SEVERITY[priority[0]]
    return Severity.warning


def to_incident(record: dict[str, Any], tenant_id: str = "default") -> Incident:
    """Map a ServiceNow incident onto the pipeline contract."""
    sys_id = _display(record.get("sys_id"))
    number = _display(record.get("number")) or sys_id
    short_description = _display(record.get("short_description")) or number
    opened_at = _parse_timestamp(_display(record.get("opened_at")))
    updated_at = _parse_timestamp(_display(record.get("sys_updated_on")))

    timeline: list[dict[str, Any]] = [
        {
            "timestamp": opened_at.isoformat(),
            "severity": derive_severity(record).value,
            "message": short_description,
            "reasons": [f"servicenow={number}"],
        }
    ]
    description = _display(record.get("description"))
    if description and description != short_description:
        timeline.append(
            {
                "timestamp": opened_at.isoformat(),
                "severity": derive_severity(record).value,
                "message": description,
                "reasons": ["servicenow=description"],
            }
        )

    return Incident(
        severity=derive_severity(record),
        service=derive_service(record),
        tenant_id=tenant_id,
        created_at=opened_at,
        updated_at=updated_at,
        summary=f"{number}: {short_description}",
        timeline=timeline,
        source_system=SOURCE_SYSTEM,
        external_id=sys_id,
    )


class ServiceNowSource(Source):
    """Polls the Table API for incidents changed since the last cursor."""

    kind = SourceKind.incidents

    def __init__(
        self,
        *,
        instance_url: str,
        username: str,
        password: str,
        tenant_id: str = "default",
        page_size: int = 50,
        timeout_seconds: float = 20.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.name = "servicenow"
        self.base_url = instance_url.rstrip("/")
        self.username = username
        self.password = password
        self.tenant_id = tenant_id
        self.page_size = page_size
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            auth=(username, password),
        )

    def build_query(self, cursor: SourceCursor) -> str:
        """Changed since the cursor, excluding our own writes.

        The second clause is the loop guard and it is not optional. Writing a
        work note updates sys_updated_on, so without it the next poll re-reads
        the incident AIRS just wrote to, generates a fresh RCA, writes another
        work note, and never stops.
        """
        clauses = [f"sys_updated_by!={self.username}"]
        if cursor.token:
            clauses.append(f"sys_updated_on>{cursor.token}")
        clauses.append("ORDERBYsys_updated_on")
        return "^".join(clauses)

    async def fetch(self, cursor: SourceCursor) -> tuple[list[SourceRecord], SourceCursor]:
        response = await self._client.get(
            f"{self.base_url}/api/now/table/incident",
            params={
                "sysparm_query": self.build_query(cursor),
                "sysparm_fields": INCIDENT_FIELDS,
                "sysparm_limit": str(self.page_size),
                "sysparm_display_value": "all",
            },
        )
        response.raise_for_status()
        rows = response.json().get("result", [])

        records: list[SourceRecord] = []
        watermark = cursor.token
        for row in rows:
            sys_id = _display(row.get("sys_id"))
            if not sys_id:
                logger.warning("Skipping ServiceNow row with no sys_id")
                continue
            records.append(
                SourceRecord(
                    payload=to_incident(row, self.tenant_id).model_dump(mode="json"),
                    kind=SourceKind.incidents,
                    tenant_id=self.tenant_id,
                    external_id=sys_id,
                    metadata={"number": _display(row.get("number"))},
                )
            )
            updated = _display(row.get("sys_updated_on"))
            if updated and (watermark is None or updated > watermark):
                watermark = updated

        return records, SourceCursor(token=watermark, last_polled_at=datetime.now(UTC))

    async def health(self) -> bool:
        try:
            response = await self._client.get(
                f"{self.base_url}/api/now/table/incident",
                params={"sysparm_limit": "1", "sysparm_fields": "sys_id"},
            )
            return response.status_code < 400
        except Exception:  # noqa: BLE001
            return False

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def format_work_note(payload: SinkPayload) -> str:
    """Plain text, because a work note is read by a human on a ticket."""
    rca = payload.rca
    lines = [
        "AIRS automated root cause analysis",
        "",
        f"Root cause: {rca.get('root_cause', 'not determined')}",
        f"Confidence: {rca.get('confidence', 0):.2f}",
        "",
        f"Analysis: {rca.get('explanation', '')}",
        "",
        f"Suggested fix: {rca.get('suggested_fix', '')}",
    ]

    affected = rca.get("affected_services") or []
    if affected:
        lines += ["", f"Affected services: {', '.join(affected)}"]

    evidence = rca.get("evidence") or []
    if evidence:
        lines += ["", "Evidence:"]
        lines += [
            f"  [{item.get('timestamp', '')}] {item.get('service', '')}: {item.get('message', '')}"
            for item in evidence[:5]
        ]

    lines += ["", f"AIRS incident: {payload.incident_id}"]
    return "\n".join(lines)


class ServiceNowWorkNoteSink(Sink):
    """Writes the RCA back to the originating ticket as a work note."""

    def __init__(
        self,
        *,
        instance_url: str,
        username: str,
        password: str,
        dry_run: bool = True,
        adapter: OutboundAdapter | None = None,
        timeout_seconds: float = 20.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.name = "servicenow-work-note"
        self.base_url = instance_url.rstrip("/")
        self.dry_run = dry_run
        self.adapter = adapter or OutboundAdapter()
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            auth=(username, password),
        )

    async def deliver(self, payload: SinkPayload) -> SinkResult:
        if payload.source_system != SOURCE_SYSTEM or not payload.external_id:
            # Not ours. A pipeline-originated incident has no ticket to update.
            return SinkResult(
                delivered=True,
                sink=self.name,
                detail="skipped, incident did not originate in ServiceNow",
                attempts=0,
                skipped_duplicate=False,
            )

        note = format_work_note(payload)

        if self.dry_run:
            logger.info(
                "DRY RUN, would write work note to ServiceNow %s:\n%s",
                payload.external_id,
                note,
            )
            return SinkResult(
                delivered=True,
                sink=self.name,
                detail=f"dry run, {len(note)} chars not written",
                attempts=0,
            )

        async def write() -> None:
            response = await self._client.patch(
                f"{self.base_url}/api/now/table/incident/{payload.external_id}",
                json={"work_notes": note},
            )
            response.raise_for_status()

        return await self.adapter.send(
            write,
            idempotency_key=payload.idempotency_key,
            description=f"{self.name}:{payload.external_id}",
        )

    async def health(self) -> bool:
        try:
            response = await self._client.get(
                f"{self.base_url}/api/now/table/incident",
                params={"sysparm_limit": "1", "sysparm_fields": "sys_id"},
            )
            return response.status_code < 400
        except Exception:  # noqa: BLE001
            return False

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
