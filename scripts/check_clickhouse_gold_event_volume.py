"""Kiểm tra Gold event volume serving table trong ClickHouse."""

from base64 import b64encode
from urllib.parse import quote
from urllib.request import Request, urlopen


CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"
CLICKHOUSE_TABLE = "bluesky.gold_event_volume_by_type"


def build_auth_header() -> str:
    """Tạo HTTP Basic Auth header cho ClickHouse local.

    Input lấy từ user/password cấu hình trong script.
    Output là giá trị header Authorization.
    """
    token = b64encode(
        f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")
    ).decode("utf-8")
    return f"Basic {token}"


def execute_clickhouse(query: str) -> str:
    """Gửi query đọc tới ClickHouse qua HTTP.

    Input chính là câu SQL cần chạy.
    Output là response text từ ClickHouse.
    """
    url = f"{CLICKHOUSE_URL}/?query={quote(query)}"
    request = Request(url, method="POST")
    request.add_header("Authorization", build_auth_header())

    with urlopen(request) as response:
        return response.read().decode("utf-8")


def main() -> None:
    """Query Gold serving table và in kết quả kiểm chứng."""
    # Đọc dữ liệu aggregate đã load vào ClickHouse.
    result = execute_clickhouse(
        f"""
        SELECT event_type, event_count
        FROM {CLICKHOUSE_TABLE}
        ORDER BY event_type
        """
    )

    print(result)


if __name__ == "__main__":
    main()