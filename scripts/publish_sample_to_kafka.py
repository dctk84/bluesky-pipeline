import json
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from bluesky_pipeline.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_EVENTS_TOPIC,
)


SAMPLE_PATH = Path("data/probe/jetstream_sample.jsonl")

def delivery_report(error: Any, message: Any) -> None:
    """In kết quả Kafka trả về sau khi producer gửi message."""
    # Nếu Kafka trả lỗi, in lỗi để biết message chưa được ghi thành công.
    if error is not None:
        print(f"delivery failed: {error}")
        return

    # Nếu thành công, in topic/partition/offset để kiểm chứng.
    print(
        "delivered: "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def read_first_event() -> dict[str, Any]:
    """Đọc event envelope đầu tiên từ sample JSONL local."""
    # Lấy một dòng đầu tiên để thử luồng publish đơn giản trước.
    with SAMPLE_PATH.open("r", encoding="utf-8") as sample_file:
        return json.loads(sample_file.readline())


def main() -> None:
    """Publish một event envelope sample vào Kafka raw topic."""
    # Tạo producer trỏ tới Kafka bootstrap servers từ cấu hình chung.
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # Đọc event envelope và dùng repository_did làm message key.
    event = read_first_event()
    key = event.get("repository_did")
    value = json.dumps(event, ensure_ascii=False)

    # Gửi message vào raw topic; callback sẽ in kết quả sau khi flush.
    producer.produce(
        KAFKA_RAW_EVENTS_TOPIC,
        key=key,
        value=value,
        callback=delivery_report,
    )

    # Chờ producer gửi hết message đang buffer.
    producer.flush()


if __name__ == "__main__":
    main()
