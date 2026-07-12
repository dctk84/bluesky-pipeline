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
) -> None:
    """Chạy một module và dừng pipeline nếu module đó lỗi."""
    command = [sys.executable, "-m", module_name]

    if ignore_state and supports_ignore_state:
        command.append("--ignore-state")

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

    for step_name, module_name, supports_ignore_state in LAKEHOUSE_INCREMENTAL_STEPS:
        run_module(
            step_name,
            module_name,
            ignore_state=args.ignore_state,
            supports_ignore_state=supports_ignore_state,
        )

    print("\nLakehouse incremental path passed")


if __name__ == "__main__":
    main()
