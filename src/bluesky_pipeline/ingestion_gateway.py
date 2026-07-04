import os
import asyncio
import json
from typing import Any

import websockets
from confluent_kafka import Producer

from bluesky_pipeline.event_envelope import build_event_envelope


WANTED_COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
]

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe?"
    + "&".join(f"wantedCollections={collection}" for collection in WANTED_COLLECTIONS)
)

TOPIC = os.getenv("KAFKA_TOPIC", "bluesky.raw.events.v1")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "100"))

delivery_failed = 0


def delivery_report(error: Any, message: Any) -> None:
    """Ghi nhận lỗi delivery nếu Kafka không nhận được message."""
    global delivery_failed

    # Chỉ in lỗi để không spam log khi gửi thành công.
    if error is not None:
        delivery_failed += 1
        print(f"delivery failed: {error}")


async def run_gateway() -> None:
    """Đọc live Jetstream event, bọc envelope và publish vào Kafka raw topic."""
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    event_count = 0

    # Kết nối Jetstream và nhận event live theo collection thuộc scope.
    async with websockets.connect(JETSTREAM_URL) as websocket:
        async for message in websocket:
            # Decode raw event rồi bọc metadata ingestion.
            raw_event = json.loads(message)
            envelope = build_event_envelope(raw_event)

            # Dùng repository_did làm Kafka key để giữ ordering tương đối.
            key = envelope.get("repository_did")
            value = json.dumps(envelope, ensure_ascii=False)

            producer.produce(
                TOPIC,
                key=key,
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)

            event_count += 1

            # Dừng hữu hạn để kiểm chứng gateway local.
            if event_count >= MAX_EVENTS:
                break

    producer.flush()

    print(f"topic: {TOPIC}")
    print(f"published_events: {event_count}")
    print(f"delivery_failed: {delivery_failed}")


if __name__ == "__main__":
    asyncio.run(run_gateway())