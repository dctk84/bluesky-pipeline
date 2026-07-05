import asyncio
import json
import logging
import os
from typing import Any

import websockets
from confluent_kafka import Producer

from bluesky_pipeline.event_envelope import build_event_envelope
from bluesky_pipeline.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_EVENTS_TOPIC,
)


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


MAX_EVENTS = int(os.getenv("MAX_EVENTS", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = int(os.getenv("RETRY_BACKOFF_SECONDS", "5"))

delivery_failed = 0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


def delivery_report(error: Any, message: Any) -> None:
    """Ghi nhận lỗi delivery nếu Kafka không nhận được message."""
    global delivery_failed

    # Chỉ log lỗi để không spam log khi gửi thành công.
    if error is not None:
        delivery_failed += 1
        logger.error("delivery failed: %s", error)


async def publish_events_once(producer: Producer, published_events: int) -> int:
    """Kết nối Jetstream một lần và publish event cho tới khi đủ MAX_EVENTS."""
    # Mỗi lần gọi hàm này tương ứng với một WebSocket connection.
    async with websockets.connect(JETSTREAM_URL) as websocket:
        async for message in websocket:
            # Decode raw event rồi bọc metadata ingestion.
            raw_event = json.loads(message)
            envelope = build_event_envelope(raw_event)

            # Dùng repository_did làm Kafka key để giữ ordering tương đối.
            key = envelope.get("repository_did")
            value = json.dumps(envelope, ensure_ascii=False)

            producer.produce(
                KAFKA_RAW_EVENTS_TOPIC,
                key=key,
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)

            published_events += 1

            # Dừng hữu hạn để kiểm chứng gateway local.
            if published_events >= MAX_EVENTS:
                return published_events

    return published_events


async def run_gateway() -> None:
    """Đọc live Jetstream event, bọc envelope và publish vào Kafka raw topic."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    published_events = 0
    retry_count = 0

    try:
        # Retry hữu hạn để tránh gateway loop vô hạn khi nguồn lỗi liên tục.
        while published_events < MAX_EVENTS:
            try:
                published_events = await publish_events_once(producer, published_events)
                retry_count = 0

            except websockets.exceptions.WebSocketException as error:
                retry_count += 1
                logger.warning(
                    "jetstream websocket error retry=%s max_retries=%s error=%s",
                    retry_count,
                    MAX_RETRIES,
                    error,
                )

                if retry_count > MAX_RETRIES:
                    logger.error("max retries exceeded")
                    raise

                await asyncio.sleep(RETRY_BACKOFF_SECONDS)

    except asyncio.CancelledError:
        # Cho phép task bị cancel nhưng vẫn flush producer trước khi thoát.
        logger.warning("gateway cancelled")
        raise

    finally:
        # Flush trong finally để giảm rủi ro mất message còn nằm trong buffer.
        producer.flush()

        logger.info(
            "gateway finished topic=%s published_events=%s delivery_failed=%s",
            KAFKA_RAW_EVENTS_TOPIC,
            published_events,
            delivery_failed,
        )


if __name__ == "__main__":
    try:
        asyncio.run(run_gateway())
    except KeyboardInterrupt:
        logger.warning("gateway interrupted")