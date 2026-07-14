import json
from pathlib import Path

from bluesky_pipeline.transforms.normalize_event import normalize_event


SAMPLE_PATH = Path("data/probe/jetstream_sample.jsonl")
OUTPUT_PATH = Path("data/probe/jetstream_normalized_sample.jsonl")


def main() -> None:
    """Đọc sample envelope JSONL và ghi ra sample đã normalize."""
    # Đảm bảo thư mục output tồn tại trước khi ghi file normalized.
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Đọc từng envelope, normalize, rồi ghi mỗi record phẳng trên một dòng.
    with SAMPLE_PATH.open("r", encoding="utf-8") as input_file:
        with OUTPUT_PATH.open("w", encoding="utf-8") as output_file:
            for line in input_file:
                envelope = json.loads(line)
                normalized_event = normalize_event(envelope)

                output_file.write(
                    json.dumps(normalized_event, ensure_ascii=False) + "\n"
                )


if __name__ == "__main__":
    main()