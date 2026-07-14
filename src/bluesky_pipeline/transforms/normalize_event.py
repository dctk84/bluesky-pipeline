from typing import Any


def normalize_event(envelope: dict[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa một event envelope thành record phẳng để pipeline xử lý tiếp."""
    # Lấy payload gốc từ envelope; nếu thiếu payload thì dùng dict rỗng.
    payload = envelope.get("payload") or {}

    # Lấy commit metadata; delete event có thể không có record.
    commit = payload.get("commit") or {}
    record = commit.get("record") or {}

    # Lấy subject nếu record là object; post create thường không có subject.
    subject = record.get("subject") if isinstance(record, dict) else None

    # Chuẩn hóa subject vì like/repost là dict, follow là string.
    if isinstance(subject, dict):
        subject_uri = subject.get("uri")
        subject_cid = subject.get("cid")
    else:
        subject_uri = subject
        subject_cid = None

    # Trả về record phẳng, giữ các field quan trọng cho Bronze/Silver sau này.
    return {
        "schema_version": envelope.get("schema_version"),
        "source": envelope.get("source"),
        "received_at": envelope.get("received_at"),
        "repository_did": envelope.get("repository_did"),
        "jetstream_time_us": envelope.get("jetstream_time_us"),
        "collection": envelope.get("collection"),
        "operation": envelope.get("operation"),
        "rkey": commit.get("rkey"),
        "cid": commit.get("cid"),
        "record_type": record.get("$type") if isinstance(record, dict) else None,
        "record_created_at": record.get("createdAt") if isinstance(record, dict) else None,
        "text": record.get("text") if isinstance(record, dict) else None,
        "subject_uri": subject_uri,
        "subject_cid": subject_cid,
        "raw_event": payload,
    }