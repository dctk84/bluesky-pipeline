from bluesky_pipeline.event_envelope import build_event_envelope


def test_build_event_envelope_adds_expected_metadata():
    raw_event = {
        "did": "did:plc:test",
        "time_us": 123456789,
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
    assert envelope["collection"] == "app.bsky.feed.post"
    assert envelope["operation"] == "create"
    assert envelope["repository_did"] == "did:plc:test"
    assert envelope["jetstream_time_us"] == 123456789
    assert envelope["payload"] == raw_event
    assert envelope["received_at"].endswith("Z")