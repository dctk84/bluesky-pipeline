"""Chạy lakehouse incremental path end-to-end cho project."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

LAKEHOUSE_INCREMENTAL_STEPS = [
    (
        "Check Silver Iceberg v1",
        "scripts.lakehouse.check_iceberg_silver_v1",
        False,
    ),
    (
        "Refresh Gold incremental",
        "scripts.gold.refresh_gold_incremental",
        True,
    ),
]

LIVE_LAKEHOUSE_INCREMENTAL_STEPS = [
    (
        "Check Silver Iceberg readiness",
        "scripts.lakehouse.check_silver_iceberg_readiness",
        False,
    ),
    (
        "Refresh Gold incremental",
        "scripts.gold.refresh_gold_incremental",
        True,
    ),
]


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho lakehouse incremental path."""
    parser = argparse.ArgumentParser(
        description="Run incremental lakehouse Gold path end-to-end."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help=(
            "Truyền `--ignore-state` xuống Gold incremental refresh. "
            "Dùng khi cần bootstrap hoặc verify lại toàn bộ incremental path."
        ),
    )
    parser.add_argument(
        "--live-mode",
        action="store_true",
        help=(
            "Dùng readiness check cho Silver thay vì full reconciliation. "
            "Phù hợp khi Bronze/Silver streaming vẫn đang ghi dữ liệu mới."
        ),
    )
    parser.add_argument(
        "--gold-mode",
        choices=["fast", "standard", "strict"],
        default=None,
        help=(
            "Truyền mode xuống Gold incremental refresh. "
            "`fast` dùng cho demo dashboard thường xuyên, `standard` thêm "
            "Trino Gold modeled check, `strict` thêm full serving reconciliation."
        ),
    )
    parser.add_argument(
        "--force-gold-downstream",
        action="store_true",
        help=(
            "Ép Gold incremental chạy dimensions và serving marts kể cả khi "
            "Gold facts không có rows mới. Dùng khi cần debug/reconcile."
        ),
    )
    return parser.parse_args()


def build_env() -> dict[str, str]:
    """Tạo environment cho process con chạy trong local project."""
    return {
        **os.environ.copy(),
        "PYTHONPATH": "src:.",
    }


def run_module(
    step_name: str,
    module_name: str,
    ignore_state: bool,
    supports_ignore_state: bool,
    extra_args: list[str] | None = None,
) -> None:
    """Chạy một module và dừng pipeline nếu module đó lỗi."""
    command = [sys.executable, "-m", module_name]

    if ignore_state and supports_ignore_state:
        command.append("--ignore-state")

    if extra_args:
        command.extend(extra_args)

    print(f"\n=== {step_name} ===", flush=True)
    print(f"command: {' '.join(command)}", flush=True)

    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        env=build_env(),
    )


def main() -> None:
    """Chạy lakehouse incremental path theo thứ tự có checkpoint."""
    args = parse_args()
    steps = (
        LIVE_LAKEHOUSE_INCREMENTAL_STEPS
        if args.live_mode
        else LAKEHOUSE_INCREMENTAL_STEPS
    )
    gold_extra_args = []

    if args.gold_mode is not None:
        gold_extra_args.extend(["--mode", args.gold_mode])

    if args.force_gold_downstream:
        gold_extra_args.append("--force-downstream")

    for step_name, module_name, supports_ignore_state in steps:
        extra_args = (
            gold_extra_args
            if module_name == "scripts.gold.refresh_gold_incremental"
            else None
        )
        run_module(
            step_name,
            module_name,
            ignore_state=args.ignore_state,
            supports_ignore_state=supports_ignore_state,
            extra_args=extra_args,
        )

    print("\nLakehouse incremental path passed")


if __name__ == "__main__":
    main()
