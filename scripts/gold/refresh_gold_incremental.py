"""Orchestrate incremental refresh cho toàn bộ Gold layer."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_INCREMENTAL_STEPS = [
    (
        "Refresh Gold facts incremental",
        "scripts.gold.refresh_gold_facts_incremental",
        True,
    ),
    (
        "Refresh Gold dimensions incremental",
        "scripts.gold.refresh_gold_dimensions_incremental",
        True,
    ),
    (
        "Check Trino Gold modeled v1",
        "scripts.lakehouse.check_trino_gold_modeled_v1",
        False,
    ),
    (
        "Refresh Gold content quality hourly incremental",
        "scripts.gold.refresh_gold_content_quality_hourly_incremental",
        True,
    ),
    (
        "Refresh Gold post performance incremental",
        "scripts.gold.refresh_gold_post_performance_incremental",
        True,
    ),
    (
        "Refresh Gold thread conversation summary incremental",
        "scripts.gold.refresh_gold_thread_conversation_summary_incremental",
        True,
    ),
    (
        "Refresh Gold network growth daily incremental",
        "scripts.gold.refresh_gold_network_growth_daily_incremental",
        True,
    ),
    (
        "Refresh Gold actor activity daily incremental",
        "scripts.gold.refresh_gold_actor_activity_daily_incremental",
        True,
    ),
]


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho Gold incremental orchestrator."""
    parser = argparse.ArgumentParser(
        description="Run Gold modeled and serving incremental refresh end-to-end."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help=(
            "Bỏ qua local state cho các incremental refresh steps. "
            "Dùng cho initial/bootstrap verification."
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
    """Chạy một module và fail-fast nếu module đó lỗi.

    Input là tên bước, module Python và flag có truyền `--ignore-state` hay không.
    Output là process con hoàn tất thành công hoặc exception từ subprocess.
    """
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
    """Chạy toàn bộ Gold incremental refresh theo đúng thứ tự."""
    args = parse_args()

    for step_name, module_name, supports_ignore_state in GOLD_INCREMENTAL_STEPS:
        run_module(
            step_name,
            module_name,
            ignore_state=args.ignore_state,
            supports_ignore_state=supports_ignore_state,
        )

    print("\nGold incremental refresh passed")


if __name__ == "__main__":
    main()
