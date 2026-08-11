from __future__ import annotations

from typing import Any

from opensearchpy import OpenSearch

INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            "created_at": {"type": "date"},
            "updated_at": {"type": "date"},
            "timestamp": {"type": "date"},
            "tenant_id": {"type": "keyword"},
            "severity": {"type": "keyword"},
            "service": {"type": "keyword"},
            "status": {"type": "keyword"},
            "parent_incident_id": {"type": "keyword"},
            "child_incident_ids": {"type": "keyword"},
            "related_services": {"type": "keyword"},
            "upstream": {"type": "keyword"},
            "downstream": {"type": "keyword"},
            "dependency_type": {"type": "keyword"},
            "message": {"type": "text"},
        }
    }
}


def build_client(url: str) -> OpenSearch:
    return OpenSearch(hosts=[url])


def ensure_index(client: OpenSearch, index_name: str) -> None:
    if client.indices.exists(index=index_name):
        return
    client.indices.create(index=index_name, body=INDEX_MAPPING)


def upsert_doc(client: OpenSearch, index_name: str, doc_id: str, body: dict[str, Any]) -> None:
    client.index(index=index_name, id=doc_id, body=body, refresh=True)


def ensure_retention_policy(
    client: OpenSearch,
    *,
    index_name: str,
    retention_days: int,
) -> None:
    policy_id = f"{index_name}-retention-policy"
    policy = {
        "policy": {
            "description": f"Auto-delete {index_name} docs after {retention_days} days",
            "default_state": "hot",
            "states": [
                {
                    "name": "hot",
                    "actions": [],
                    "transitions": [
                        {
                            "state_name": "delete",
                            "conditions": {"min_index_age": f"{retention_days}d"},
                        }
                    ],
                },
                {
                    "name": "delete",
                    "actions": [{"delete": {}}],
                    "transitions": [],
                },
            ],
        }
    }

    # OpenSearch Index State Management endpoints are optional in some environments.
    try:
        client.transport.perform_request(
            method="PUT",
            url=f"/_plugins/_ism/policies/{policy_id}",
            body=policy,
        )
    except Exception:  # noqa: BLE001
        return

    try:
        client.transport.perform_request(
            method="POST",
            url=f"/_plugins/_ism/add/{index_name}",
            body={"policy_id": policy_id},
        )
    except Exception:  # noqa: BLE001
        return


def build_async_client(url: str) -> Any:
    """Async OpenSearch client for use inside event loops.

    The synchronous client blocks the entire loop for the duration of every
    call. In a consumer that is a throughput ceiling; in the SSE endpoint it is
    a blocking search every two seconds per connected client on the gateway's
    only loop. Measured consequence in docs/06-evals.md: the pipeline sustains
    roughly 200 events/sec against a CPU-bound ceiling near 500,000.
    """
    from opensearchpy import AsyncOpenSearch

    return AsyncOpenSearch(hosts=[url])


async def bulk_index(
    client: Any,
    index_name: str,
    documents: list[tuple[str, dict[str, Any]]],
    *,
    refresh: bool = False,
) -> int:
    """Index a batch in one request. Returns how many were accepted.

    Two changes from the per-document path this replaces. One request instead
    of N, and `refresh=False` by default: forcing a refresh per document made
    every write pay for a segment flush, which was the single largest cost in
    the ingest path. Search becomes visible on OpenSearch's own refresh
    interval instead, which is the normal trade.
    """
    if not documents:
        return 0

    body: list[dict[str, Any]] = []
    for doc_id, source in documents:
        body.append({"index": {"_index": index_name, "_id": doc_id}})
        body.append(source)

    response = await client.bulk(body=body, refresh=refresh)
    if not response.get("errors"):
        return len(documents)

    failed = sum(1 for item in response.get("items", []) if item.get("index", {}).get("error"))
    return len(documents) - failed


async def async_ensure_index(client: Any, index_name: str) -> None:
    if await client.indices.exists(index=index_name):
        return
    await client.indices.create(index=index_name, body=INDEX_MAPPING)
