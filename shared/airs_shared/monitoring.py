from __future__ import annotations

from fastapi import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


def metrics_response() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
