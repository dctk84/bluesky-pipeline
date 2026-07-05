"""Helper gửi query tới ClickHouse local qua HTTP API."""

import os
from base64 import b64encode
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError


CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")


def build_auth_header() -> str:
    """Tạo HTTP Basic Auth header cho ClickHouse.

    Input lấy từ environment variables hoặc giá trị mặc định local.
    Output là giá trị header Authorization.
    """
    token = b64encode(
        f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")
    ).decode("utf-8")
    return f"Basic {token}"


def execute_clickhouse(query: str, body: str | None = None) -> str:
    """Gửi query tới ClickHouse qua HTTP.

    Input chính là câu SQL và body optional cho INSERT.
    Output là response text từ ClickHouse.
    Nếu ClickHouse trả lỗi, hàm sẽ hiển thị nội dung lỗi từ server.
    """
    url = f"{CLICKHOUSE_URL}/?query={quote(query)}"
    data = body.encode("utf-8") if body is not None else None

    request = Request(url, data=data, method="POST")
    request.add_header("Authorization", build_auth_header())

    try:
        # Gửi request tới ClickHouse và đọc response text khi thành công.
        with urlopen(request) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        # ClickHouse thường trả thông tin lỗi chi tiết trong response body.
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            "ClickHouse query failed\n"
            f"status_code: {error.code}\n"
            f"reason: {error.reason}\n"
            f"query:\n{query.strip()}\n"
            f"response:\n{error_body.strip()}"
        ) from error