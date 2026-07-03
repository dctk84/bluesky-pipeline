import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SAMPLE_PATH = Path("data/probe/jetstream_sample.jsonl")


def value_shape(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return type(value).__name__


def main() -> None:
    collection_counts: Counter[str] = Counter()
    operation_counts: Counter[str] = Counter()
    record_type_counts: Counter[str] = Counter()
    subject_shape_counts: Counter[str] = Counter()

    # Tạo dictionary để gom danh sách field xuất hiện trong record của từng collection
    # defaultdict(set) giúp tự tạo set rỗng nếu collection chưa tồn tại
    record_fields_by_collection: dict[str, set[str]] = defaultdict(set)

    with SAMPLE_PATH.open("r", encoding="utf-8") as sample_file:
        for line in sample_file:
            # Lấy từng dòng thành json
            event = json.loads(line)

            # Lấy nội dung commit từ dòng
            commit = event.get("commit", {})

            # Lấy record trong commit, nếu không có thì dùng {}
            record = commit.get("record") or {}

            # Lấy tên collection, nếu thiếu thì gán là "unknown"
            collection = commit.get("collection", "unknown")

            # Lấy loại thao tác: create/update/delete, nếu thiếu thì gán là "unknown"
            operation = commit.get("operation", "unknown")

            # Lấy $type trong record, nếu thiếu thì gán là "missing"
            record_type = record.get("$type", "missing")

            # Lấy field subject trong record, có thể là dict, string, list, None,...
            subject = record.get("subject")
            
            # Tăng bộ đếm cho collection hiện tại lên 1
            collection_counts[collection] += 1

            # Tăng bộ đếm cho cặp collection + operation hiên tại lên 1
            operation_counts[f"{collection}:{operation}"] += 1

            # Tăng bộ đếm cho record type hiện tại lên 1
            record_type_counts[record_type] += 1

            # Xác định shape của subject rồi đếm theo từng collection
            subject_shape_counts[f"{collection}:{value_shape(subject)}"] += 1

            # Chỉ xử lý record nếu trong record thực sử là dict
            if isinstance(record, dict):
                # Lấy toàn bộ tên field trong record hiện tại
                # rồi thêm vào set field của collection tương ứng
                record_fields_by_collection[collection].update(record.keys())

    print("collection_counts")
    print(json.dumps(collection_counts, indent=2, ensure_ascii=False))

    print("\noperation_counts")
    print(json.dumps(operation_counts, indent=2, ensure_ascii=False))

    print("\nrecord_type_counts")
    print(json.dumps(record_type_counts, indent=2, ensure_ascii=False))

    print("\nsubject_shape_counts")
    print(json.dumps(subject_shape_counts, indent=2, ensure_ascii=False))

    # In danh sách các field đã từng xuất hiện trong record của từng collection
    print("\nrecord_fields_by_collection")
    for collection, fields in sorted(record_fields_by_collection.items()):
        # In tên collection
        print(collection)

        # Sắp xếp các field theo alphabet rồi in từng field
        for field in sorted(fields):
            print(f"  - {field}")


if __name__ == "__main__":
    main()