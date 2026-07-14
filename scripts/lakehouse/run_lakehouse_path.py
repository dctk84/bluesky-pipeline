"""Chạy lakehouse path end-to-end cho project."""

from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]


STEPS = [
    (
        "Build Silver Iceberg v1",
        "scripts.lakehouse.build_iceberg_silver_v1",
    ),
    (
        "Check Silver Iceberg v1",
        "scripts.lakehouse.check_iceberg_silver_v1",
    ),
    (
        "Build Gold modeled v1",
        "scripts.lakehouse.build_gold_modeled_v1",
    ),
    (
        "Check Trino Gold modeled v1",
        "scripts.lakehouse.check_trino_gold_modeled_v1",
    ),
    (
        "Refresh Gold serving from lakehouse Iceberg",
        "scripts.gold.refresh.refresh_serving_from_iceberg",
    ),
]


def run_step(step_name: str, module_name: str) -> None:
    """Chạy một module script và dừng toàn bộ pipeline nếu module đó lỗi."""
    print(f"\n=== {step_name} ===")

    # Chạy bằng module để import package `scripts.*` ổn định sau khi tách thư mục.
    subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=PROJECT_ROOT,
        check=True,
        env={
            **dict(**__import__("os").environ),
            "PYTHONPATH": "src:.",
        },
    )


def main() -> None:
    """Chạy toàn bộ lakehouse path theo đúng thứ tự."""
    for step_name, script_path in STEPS:
        # check=True trong run_step tạo fail-fast: bước lỗi thì dừng pipeline ngay.
        run_step(step_name, script_path)

    print("\nLakehouse path passed")


if __name__ == "__main__":
    main()
