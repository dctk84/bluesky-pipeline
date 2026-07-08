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


def should_stop(published_events: int) -> bool:
    """Trả về True nếu gateway đã publish đủ số event cần chạy.

    MAX_EVENTS = 0 nghĩa là chạy live không giới hạn cho demo realtime.
    """
    return MAX_EVENTS > 0 and published_events >= MAX_EVENTS


async def publish_events_once(producer: Producer, published_events: int) -> int:
    """Kết nối Jetstream một lần và publish event cho tới khi đủ MAX_EVENTS."""
    # Mỗi lần gọi hàm này tương ứng với một WebSocket connection.
    async with websockets.connect(JETSTREAM_URL) as websocket:
        events_before_connection = published_events

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

            # Dừng hữu hạn khi MAX_EVENTS > 0; MAX_EVENTS = 0 là live mode.
            if should_stop(published_events):
                return published_events

        if published_events > events_before_connection:
            logger.info(
                "jetstream connection ended after publishing events_in_connection=%s",
                published_events - events_before_connection,
            )

    return published_events


async def run_gateway() -> None:
    """Đọc live Jetstream event, bọc envelope và publish vào Kafka raw topic."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    published_events = 0
    connection_error_count = 0
    consecutive_retry_count = 0

    try:
        # Retry hữu hạn để tránh gateway loop vô hạn khi nguồn lỗi liên tục.
        while not should_stop(published_events):
            events_before_connection = published_events

            try:
                published_events = await publish_events_once(producer, published_events)

                # Nếu connection vừa rồi đã nhận được data thì chuỗi lỗi liên tiếp đã kết thúc.
                if published_events > events_before_connection:
                    consecutive_retry_count = 0

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                OSError,
                TimeoutError,
                asyncio.TimeoutError,
            ) as error:
                connection_error_count += 1
                consecutive_retry_count += 1
                logger.warning(
                    (
                        "jetstream websocket error consecutive_retry=%s "
                        "total_connection_errors=%s max_retries=%s error=%s"
                    ),
                    consecutive_retry_count,
                    connection_error_count,
                    MAX_RETRIES,
                    error,
                )

                # MAX_RETRIES=0 dùng cho live demo: retry không giới hạn khi nguồn ngắt.
                if MAX_RETRIES > 0 and consecutive_retry_count > MAX_RETRIES:
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
            (
                "gateway finished topic=%s published_events=%s delivery_failed=%s "
                "total_connection_errors=%s"
            ),
            KAFKA_RAW_EVENTS_TOPIC,
            published_events,
            delivery_failed,
            connection_error_count,
        )


if __name__ == "__main__":
    try:
        asyncio.run(run_gateway())
    except KeyboardInterrupt:
        logger.warning("gateway interrupted")
