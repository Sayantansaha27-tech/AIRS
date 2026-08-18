"""Logging configuration, so the services can actually be watched.

Nothing configured the root logger, which defaults to WARNING. Every
`logger.info` in AIRS was therefore discarded: the dry-run preview of a
ServiceNow work note, "published N records", which state store was selected,
and every other line the runbook tells an operator to look for. Only warnings
and tracebacks ever appeared, which made a working system look silent and a
degraded one look identical to it.
"""

from __future__ import annotations

import logging
import os
import sys

DEFAULT_LEVEL = "INFO"


def configure_logging(service: str, level: str | None = None) -> logging.Logger:
    """Set up root logging once and return this service's logger.

    Level comes from AIRS_LOG_LEVEL so it can be turned up in an incident
    without a rebuild. Uvicorn installs its own handlers for access logs; this
    configures the root logger that application code writes to.
    """
    resolved = (level or os.getenv("AIRS_LOG_LEVEL") or DEFAULT_LEVEL).upper()
    numeric = getattr(logging, resolved, logging.INFO)

    root = logging.getLogger()
    if not any(getattr(h, "_airs", False) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
        handler._airs = True  # type: ignore[attr-defined]
        root.addHandler(handler)
    root.setLevel(numeric)

    # aiokafka is extremely chatty at INFO and drowns everything else.
    logging.getLogger("aiokafka").setLevel(max(numeric, logging.WARNING))

    return logging.getLogger(service)
