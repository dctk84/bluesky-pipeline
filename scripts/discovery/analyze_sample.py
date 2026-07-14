import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SAMPLE_PATH = Path("data/probe/jetstream_sample.jsonl")


def value_shape(value: Any) -> str:
    """Trả về nhãn kiểu dữ liệu ngắn gọn cho field quan sát được."""
    if value is None:
        return "null"
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return type(value).__name__


def main() -> None:
    """Profile sample Jetstream JSONL local và in ra quan sát về schema."""
    collection_counts: Counter[str] = Counter()
    operation_counts: Counter[str] = Counter()
    record_type_counts: Counter[str] = Counter()
    subject_shape_counts: Counter[str] = Counter()

    # Theo dõi toàn bộ record field từng xuất hiện theo từng collection.
    record_fields_by_collection: dict[str, set[str]] = defaultdict(set)

    with SAMPLE_PATH.open("r", encoding="utf-8") as sample_file:
        for line in sample_file:
            # Decode một dòng JSONL và hỗ trợ cả envelope lẫn raw sample cũ.
            envelope = json.loads(line)
            event = envelope.get("payload", envelope)

            # Lấy metadata ở cấp commit để đếm và profile schema.
            commit = event.get("commit", {})
            record = commit.get("record") or {}
            collection = commit.get("collection", "unknown")
            operation = commit.get("operation", "unknown")

            # Delete event có thể không có record, nên dùng nhãn fallback rõ ràng.
            record_type = record.get("$type", "missing")
            subject = record.get("subject")

            # Cập nhật các bộ đếm cho collection, operation, record type và subject.
            collection_counts[collection] += 1
            operation_counts[f"{collection}:{operation}"] += 1
            record_type_counts[record_type] += 1
            subject_shape_counts[f"{collection}:{value_shape(subject)}"] += 1

            # Chỉ gom field khi record thực sự là object.
            if isinstance(record, dict):
                record_fields_by_collection[collection].update(record.keys())

    print("collection_counts")
    print(json.dumps(collection_counts, indent=2, ensure_ascii=False))

    print("\noperation_counts")
    print(json.dumps(operation_counts, indent=2, ensure_ascii=False))

    print("\nrecord_type_counts")
    print(json.dumps(record_type_counts, indent=2, ensure_ascii=False))

    print("\nsubject_shape_counts")
    print(json.dumps(subject_shape_counts, indent=2, ensure_ascii=False))

    # In danh sách record field quan sát được theo từng collection.
    print("\nrecord_fields_by_collection")
    for collection, fields in sorted(record_fields_by_collection.items()):
        print(collection)

        # Sắp xếp field name để các lần chạy dễ so sánh hơn.
        for field in sorted(fields):
            print(f"  - {field}")


if __name__ == "__main__":
    main()
