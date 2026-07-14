from datetime import UTC, datetime
from typing import Any


SCHEMA_VERSION = 1
SOURCE = "bluesky_jetstream"


def utc_now_iso() -> str:
    """Trả về thời điểm UTC hiện tại ở dạng ISO-8601 kết thúc bằng Z."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def build_event_envelope(raw_event: dict[str, Any]) -> dict[str, Any]:
    """Bọc raw Jetstream event bằng metadata ingestion ổn định."""
    # Lấy metadata commit nếu event hiện tại là repository commit event.
    commit = raw_event.get("commit", {})

    # Giữ nguyên event gốc trong payload để phục vụ audit và replay.
    return {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "event_kind": raw_event.get("kind"),
        "received_at": utc_now_iso(),
        "collection": commit.get("collection"),
        "operation": commit.get("operation"),
        "repository_did": raw_event.get("did"),
        "jetstream_time_us": raw_event.get("time_us"),
        "payload": raw_event,
    }
