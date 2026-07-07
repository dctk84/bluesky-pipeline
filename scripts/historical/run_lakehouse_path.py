"""Chạy historical/lakehouse path end-to-end cho project."""

from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]


STEPS = [
    (
        "Build Silver Iceberg v1",
        "scripts.historical.build_iceberg_silver_v1",
    ),
    (
        "Check Silver Iceberg v1",
        "scripts.historical.check_iceberg_silver_v1",
    ),
    (
        "Refresh Gold serving from Silver Iceberg",
        "scripts.gold.refresh_serving_from_iceberg",
    ),
]


def run_step(step_name: str, module_name: str) -> None:
    """Chạy một module script và dừng toàn bộ pipeline nếu module đó lỗi."""
    print(f"\n=== {step_name} ===")

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
    """Chạy toàn bộ historical/lakehouse path theo đúng thứ tự."""
    for step_name, script_path in STEPS:
        run_step(step_name, script_path)

    print("\nHistorical lakehouse path passed")


if __name__ == "__main__":
    main()
