from __future__ import annotations

from typing import Any

from opensearchpy import OpenSearch


def build_client(url: str) -> OpenSearch:
    return OpenSearch(hosts=[url])


def ensure_index(client: OpenSearch, index_name: str) -> None:
    if client.indices.exists(index=index_name):
        return
    client.indices.create(
        index=index_name,
        body={
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
        },
    )


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
