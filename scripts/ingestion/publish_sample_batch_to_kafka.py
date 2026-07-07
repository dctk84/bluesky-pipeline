import json
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from bluesky_pipeline.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_EVENTS_TOPIC,
)


SAMPLE_PATH = Path("data/probe/jetstream_sample.jsonl")
MAX_EVENTS = 100

delivery_failed = 0


def delivery_report(error: Any, message: Any) -> None:
    """Ghi nhận lỗi delivery nếu Kafka không nhận được message."""
    global delivery_failed

    # Chỉ in lỗi để tránh spam console khi publish nhiều event thành công.
    if error is not None:
        delivery_failed += 1
        print(f"delivery failed: {error}")


def read_events(limit: int) -> list[dict[str, Any]]:
    """Đọc tối đa limit event envelope từ sample JSONL local."""
    events: list[dict[str, Any]] = []

    # Đọc tuần tự từng dòng để mô phỏng một batch nhỏ từ nguồn local.
    with SAMPLE_PATH.open("r", encoding="utf-8") as sample_file:
        for line in sample_file:
            if len(events) >= limit:
                break

            events.append(json.loads(line))

    return events


def main() -> None:
    """Publish một batch nhỏ event envelope sample vào Kafka raw topic."""
    # Tạo Kafka producer trỏ tới broker local.
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    events = read_events(MAX_EVENTS)

    # Publish từng event, dùng repository_did làm key để giữ ordering tương đối.
    for event in events:
        key = event.get("repository_did")
        value = json.dumps(event, ensure_ascii=False)

        producer.produce(
            KAFKA_RAW_EVENTS_TOPIC,
            key=key,
            value=value,
            callback=delivery_report,
        )

        # Cho producer xử lý callback delivery trong lúc đang gửi batch.
        producer.poll(0)

    # Chờ toàn bộ message trong buffer được gửi xong.
    producer.flush()

    print(f"topic: {KAFKA_RAW_EVENTS_TOPIC}")
    print(f"requested_events: {MAX_EVENTS}")
    print(f"published_events: {len(events)}")
    print(f"delivery_failed: {delivery_failed}")


if __name__ == "__main__":
    main()