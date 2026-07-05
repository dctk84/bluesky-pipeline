from bluesky_pipeline.event_envelope import build_event_envelope


def test_build_event_envelope_adds_expected_metadata_for_create_event():
    """Envelope phải giữ raw create event và expose metadata chính."""
    raw_event = {
        "did": "did:plc:test",
        "time_us": 123456789,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "hello",
            },
        },
    }

    envelope = build_event_envelope(raw_event)

    assert envelope["schema_version"] == 1
    assert envelope["source"] == "bluesky_jetstream"
    assert envelope["received_at"].endswith("Z")
    assert envelope["collection"] == "app.bsky.feed.post"
    assert envelope["operation"] == "create"
    assert envelope["repository_did"] == "did:plc:test"
    assert envelope["jetstream_time_us"] == 123456789
    assert envelope["payload"] == raw_event
    assert envelope["event_kind"] == "commit"


def test_build_event_envelope_handles_delete_event_without_record():
    """Envelope phải hỗ trợ delete event không có record."""
    raw_event = {
        "did": "did:plc:test",
        "time_us": 123456790,
        "commit": {
            "operation": "delete",
            "collection": "app.bsky.feed.like",
            "rkey": "abc123",
        },
    }

    envelope = build_event_envelope(raw_event)

    assert envelope["collection"] == "app.bsky.feed.like"
    assert envelope["operation"] == "delete"
    assert envelope["repository_did"] == "did:plc:test"
    assert envelope["jetstream_time_us"] == 123456790
    assert envelope["payload"] == raw_event


def test_build_event_envelope_handles_missing_commit():
    """Envelope phải giữ non-commit event và để trống các commit field."""
    raw_event = {
        "did": "did:plc:test",
        "time_us": 123456791,
        "kind": "identity",
    }

    envelope = build_event_envelope(raw_event)

    assert envelope["collection"] is None
    assert envelope["operation"] is None
    assert envelope["repository_did"] == "did:plc:test"
    assert envelope["jetstream_time_us"] == 123456791
    assert envelope["payload"] == raw_event
    assert envelope["event_kind"] == "identity"


def test_build_event_envelope_handles_empty_event():
    """Envelope phải xử lý được event rỗng mà không raise exception."""
    raw_event = {}

    envelope = build_event_envelope(raw_event)

    assert envelope["schema_version"] == 1
    assert envelope["source"] == "bluesky_jetstream"
    assert envelope["event_kind"] is None
    assert envelope["collection"] is None
    assert envelope["operation"] is None
    assert envelope["repository_did"] is None
    assert envelope["jetstream_time_us"] is None
    assert envelope["payload"] == raw_event
