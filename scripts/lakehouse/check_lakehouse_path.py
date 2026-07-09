"""Chạy checkpoint tổng hợp cho lakehouse path."""

from pathlib import Path
import os
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]


STEPS = [
    (
        "Check Silver Iceberg v1",
        "scripts.lakehouse.check_iceberg_silver_v1",
    ),
    (
        "Check Gold serving v1",
        "scripts.gold.check_serving_v1",
    ),
]


def run_step(step_name: str, module_name: str) -> None:
    """Chạy một checkpoint module và dừng nếu checkpoint đó lỗi."""
    print(f"\n=== {step_name} ===")

    env = os.environ.copy()
    env["PYTHONPATH"] = "src:."

    # Chạy checkpoint bằng subprocess để mỗi bước giữ nguyên hành vi CLI độc lập.
    subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=PROJECT_ROOT,
        check=True,
        env=env,
    )


def main() -> None:
    """Chạy toàn bộ checkpoint của lakehouse path."""
    for step_name, module_name in STEPS:
        # Nếu một checkpoint fail, subprocess.run(check=True) dừng toàn bộ check.
        run_step(step_name, module_name)

    print("\nLakehouse path check passed")


if __name__ == "__main__":
    main()
