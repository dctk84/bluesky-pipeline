"""Orchestrate incremental refresh cho toàn bộ Gold layer."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODE_ENV = "GOLD_INCREMENTAL_MODE"
SKIP_DOWNSTREAM_ENV = "GOLD_INCREMENTAL_SKIP_DOWNSTREAM_ON_NO_FACT_CHANGES"
MODE_FAST = "fast"
MODE_STANDARD = "standard"
MODE_STRICT = "strict"
SUPPORTED_MODES = {MODE_FAST, MODE_STANDARD, MODE_STRICT}

GOLD_FACT_STEP = (
    "Refresh Gold facts incremental",
    "scripts.gold.refresh_gold_facts_incremental",
    True,
)

GOLD_DIMENSION_STEP = (
    "Refresh Gold dimensions incremental",
    "scripts.gold.refresh_gold_dimensions_incremental",
    True,
)

GOLD_SERVING_STEPS = [
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

GOLD_MODELED_VALIDATION_STEP = (
    "Check Trino Gold modeled v1",
    "scripts.lakehouse.check_trino_gold_modeled_v1",
    False,
)

GOLD_SERVING_VALIDATION_STEP = (
    "Check Gold serving v1",
    "scripts.gold.check_serving_v1",
    False,
)


@dataclass(frozen=True)
class StepResult:
    """Kết quả một bước orchestration.

    Input được tạo từ process con vừa chạy.
    Output gồm stdout/stderr đã capture và thời gian chạy để quan sát bottleneck.
    """

    output_lines: list[str]
    duration_seconds: float


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
    parser.add_argument(
        "--mode",
        choices=sorted(SUPPORTED_MODES),
        default=None,
        help=(
            "Chế độ chạy Gold incremental. "
            "`fast` dùng cho refresh thường xuyên, `standard` thêm Trino check, "
            "`strict` thêm full serving reconciliation."
        ),
    )
    parser.add_argument(
        "--force-downstream",
        action="store_true",
        help=(
            "Vẫn chạy dimensions, Trino check và serving marts kể cả khi "
            "Gold facts không có rows mới. Dùng khi cần reconcile/debug."
        ),
    )
    return parser.parse_args()


def parse_bool_env(name: str, default_value: bool) -> bool:
    """Đọc biến môi trường dạng boolean đơn giản."""
    raw_value = os.getenv(name)

    if raw_value is None:
        return default_value

    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_mode(args: argparse.Namespace) -> str:
    """Xác định mode Gold incremental từ CLI/env/default.

    Input là args CLI.
    Output là một trong `fast`, `standard`, `strict`.
    """
    raw_mode = args.mode or os.getenv(MODE_ENV)

    if raw_mode is None:
        # Bootstrap initial run cần checkpoint kỹ hơn; vòng incremental thường
        # xuyên ưu tiên latency nên dùng fast mode.
        return MODE_STANDARD if args.ignore_state else MODE_FAST

    mode = raw_mode.strip().lower()

    if mode not in SUPPORTED_MODES:
        supported_modes = ", ".join(sorted(SUPPORTED_MODES))
        raise ValueError(f"{MODE_ENV} must be one of: {supported_modes}")

    return mode


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
) -> StepResult:
    """Chạy một module và fail-fast nếu module đó lỗi.

    Input là tên bước, module Python và flag có truyền `--ignore-state` hay không.
    Output là StepResult gồm stdout/stderr và duration.
    """
    command = [sys.executable, "-m", module_name]

    if ignore_state and supports_ignore_state:
        command.append("--ignore-state")

    print(f"\n=== {step_name} ===", flush=True)
    print(f"command: {' '.join(command)}", flush=True)

    started_at = time.monotonic()
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=build_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output_lines: list[str] = []

    assert process.stdout is not None

    for line in process.stdout:
        output_lines.append(line.rstrip("\n"))
        print(line, end="", flush=True)

    return_code = process.wait()
    duration_seconds = time.monotonic() - started_at
    print(f"{step_name}_duration_seconds: {duration_seconds:.2f}", flush=True)

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)

    return StepResult(
        output_lines=output_lines,
        duration_seconds=duration_seconds,
    )


def facts_output_has_changes(output_lines: list[str]) -> bool:
    """Xác định Gold facts job có append rows mới hay không.

    Input là stdout/stderr của facts step.
    Output là False khi facts step in marker `gold_facts_has_changes: false`.
    Nếu marker không xuất hiện, trả True để giữ hành vi an toàn.
    """
    for line in reversed(output_lines):
        if line.startswith("gold_facts_has_changes:"):
            return line.split(":", 1)[1].strip().lower() == "true"

    return True


def print_step_duration_summary(step_results: list[tuple[str, StepResult]]) -> None:
    """In bảng duration của các bước Gold incremental.

    Input là list tên bước và StepResult.
    Output là log summary giúp tìm bottleneck sau mỗi lần chạy.
    """
    if not step_results:
        return

    total_seconds = sum(result.duration_seconds for _, result in step_results)
    print("\n=== gold_incremental_step_durations ===", flush=True)

    for step_name, result in step_results:
        print(
            f"{step_name}\t{result.duration_seconds:.2f}",
            flush=True,
        )

    print(f"gold_incremental_total_duration_seconds: {total_seconds:.2f}", flush=True)


def main() -> None:
    """Chạy toàn bộ Gold incremental refresh theo đúng thứ tự."""
    args = parse_args()
    mode = resolve_mode(args)
    skip_downstream_on_no_changes = parse_bool_env(SKIP_DOWNSTREAM_ENV, True)
    step_results: list[tuple[str, StepResult]] = []

    print(f"gold_incremental_mode: {mode}", flush=True)

    fact_step_name, fact_module_name, fact_supports_ignore_state = GOLD_FACT_STEP
    fact_result = run_module(
        fact_step_name,
        fact_module_name,
        ignore_state=args.ignore_state,
        supports_ignore_state=fact_supports_ignore_state,
    )
    step_results.append((fact_step_name, fact_result))
    facts_have_changes = facts_output_has_changes(fact_result.output_lines)

    if (
        skip_downstream_on_no_changes
        and not args.ignore_state
        and not args.force_downstream
        and mode != MODE_STRICT
        and not facts_have_changes
    ):
        # Không có fact mới thì dimensions và serving marts không đổi.
        print(
            "\nGold downstream refresh skipped: no new Gold fact rows.",
            flush=True,
        )
        print_step_duration_summary(step_results)
        print("\nGold incremental refresh passed")
        return

    step_name, module_name, supports_ignore_state = GOLD_DIMENSION_STEP
    step_result = run_module(
        step_name,
        module_name,
        ignore_state=args.ignore_state,
        supports_ignore_state=supports_ignore_state,
    )
    step_results.append((step_name, step_result))

    if mode in {MODE_STANDARD, MODE_STRICT}:
        step_name, module_name, supports_ignore_state = GOLD_MODELED_VALIDATION_STEP
        step_result = run_module(
            step_name,
            module_name,
            ignore_state=args.ignore_state,
            supports_ignore_state=supports_ignore_state,
        )
        step_results.append((step_name, step_result))

    for step_name, module_name, supports_ignore_state in GOLD_SERVING_STEPS:
        step_result = run_module(
            step_name,
            module_name,
            ignore_state=args.ignore_state,
            supports_ignore_state=supports_ignore_state,
        )
        step_results.append((step_name, step_result))

    if mode == MODE_STRICT:
        step_name, module_name, supports_ignore_state = GOLD_SERVING_VALIDATION_STEP
        step_result = run_module(
            step_name,
            module_name,
            ignore_state=args.ignore_state,
            supports_ignore_state=supports_ignore_state,
        )
        step_results.append((step_name, step_result))

    print_step_duration_summary(step_results)
    print("\nGold incremental refresh passed")


if __name__ == "__main__":
    main()
