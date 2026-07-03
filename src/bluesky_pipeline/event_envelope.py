from datetime import UTC, datetime
from typing import Any


SCHEMA_VERSION = 1
SOURCE = "bluesky_jetstream"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def build_event_envelope(raw_event: dict[str, Any]) -> dict[str, Any]:
    commit = raw_event.get("commit", {})

    return {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "received_at": utc_now_iso(),
        "collection": commit.get("collection"),
        "operation": commit.get("operation"),
        "repository_did": raw_event.get("did"),
        "jetstream_time_us": raw_event.get("time_us"),
        "payload": raw_event,
    }