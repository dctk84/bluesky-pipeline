"""Contract dùng chung cho incremental refresh jobs."""
from __future__ import annotations
from pathlib import Path

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import json

DEFAULT_LOOKBACK_HOURS = 2


@dataclass(frozen=True)
class RefreshWindow:
    """Mô tả khoảng dữ liệu cần xử lý trong một lần incremental refresh.

    Input chính gồm thời điểm bắt đầu, kết thúc và lookback.
    Output là contract dùng chung cho các Gold incremental jobs.
    """

    refresh_from: datetime
    refresh_to: datetime
    lookback_hours: int


def utc_now() -> datetime:
    """Trả về thời điểm hiện tại theo UTC timezone-aware."""
    return datetime.now(timezone.utc)


def build_refresh_window(
    last_successful_run_at: datetime | None,
    refresh_to: datetime | None = None,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
) -> RefreshWindow:
    """Tạo refresh window cho incremental job.

    Input chính là lần chạy thành công gần nhất, thời điểm kết thúc optional và
    số giờ lookback.
    Output là RefreshWindow dùng để filter dữ liệu Silver cần xử lý.
    """
    if lookback_hours < 0:
        raise ValueError("lookback_hours must be >= 0")

    effective_refresh_to = refresh_to or utc_now()

    if effective_refresh_to.tzinfo is None:
        raise ValueError("refresh_to must be timezone-aware")

    if last_successful_run_at is None:
        refresh_from = datetime.min.replace(tzinfo=timezone.utc)
    else:
        if last_successful_run_at.tzinfo is None:
            raise ValueError("last_successful_run_at must be timezone-aware")

        refresh_from = last_successful_run_at - timedelta(hours=lookback_hours)

    if refresh_from > effective_refresh_to:
        raise ValueError("refresh_from must be <= refresh_to")

    return RefreshWindow(
        refresh_from=refresh_from,
        refresh_to=effective_refresh_to,
        lookback_hours=lookback_hours,
    )

def read_last_successful_run_at(state_path: Path) -> datetime | None:
    """Đọc thời điểm incremental refresh thành công gần nhất từ local state file.

    Input là đường dẫn file JSON local.
    Output là datetime timezone-aware hoặc None nếu chưa có state.
    """
    if not state_path.exists():
        return None

    state = json.loads(state_path.read_text(encoding="utf-8"))
    value = state.get("last_successful_run_at")

    if value is None:
        return None

    parsed_value = datetime.fromisoformat(value)

    if parsed_value.tzinfo is None:
        raise ValueError("last_successful_run_at in state file must be timezone-aware")

    return parsed_value


def write_last_successful_run_at(state_path: Path, value: datetime) -> None:
    """Ghi thời điểm incremental refresh thành công gần nhất vào local state file.

    Input là đường dẫn file JSON local và datetime timezone-aware cần lưu.
    Output là file state được tạo hoặc cập nhật.
    """
    if value.tzinfo is None:
        raise ValueError("last_successful_run_at must be timezone-aware")

    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {"last_successful_run_at": value.isoformat()},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )