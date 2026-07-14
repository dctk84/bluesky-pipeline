"""Chạy live pipeline từ Bluesky Jetstream tới ClickHouse/Grafana."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "live_pipeline"
LOG_TAIL_LINES = 80

PROCESS_SPECS = [
    (
        "bronze-writer",
        [sys.executable, "scripts/ingestion/spark_read_kafka_raw.py"],
        {},
    ),
    (
        "realtime-metrics",
        [sys.executable, "scripts/realtime/stream_metrics_to_clickhouse.py"],
        {},
    ),
    (
        "silver-stream",
        [sys.executable, "scripts/lakehouse/stream_silver_from_bronze.py"],
        {},
    ),
    (
        "ingestion-gateway",
        [sys.executable, "-m", "bluesky_pipeline.ingestion_gateway"],
        # MAX_EVENTS=0 chuyển gateway sang live mode: đọc Jetstream liên tục.
        # MAX_RETRIES=0 cho phép reconnect vô hạn khi Jetstream ngắt tạm thời.
        {"MAX_EVENTS": "0", "MAX_RETRIES": "0"},
    ),
]


def build_env(extra_env: dict[str, str]) -> dict[str, str]:
    """Tạo environment cho process con chạy trong local project."""
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:."
    env.update(extra_env)
    return env


def tail_log(process_name: str) -> None:
    """In các dòng cuối của log process để debug khi process chết."""
    log_path = LOG_DIR / f"{process_name}.log"

    if not log_path.exists():
        print(f"{process_name}: log file not found: {log_path}", flush=True)
        return

    lines = log_path.read_text(errors="replace").splitlines()
    print(f"\n--- {process_name} log tail ({log_path}) ---", flush=True)
    for line in lines[-LOG_TAIL_LINES:]:
        print(line, flush=True)
    print(f"--- end {process_name} log tail ---\n", flush=True)


def start_processes() -> list[tuple[str, subprocess.Popen]]:
    """Start các process chính của live pipeline."""
    processes = []
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    for process_name, command, extra_env in PROCESS_SPECS:
        print(f"starting {process_name}: {' '.join(command)}", flush=True)
        log_path = LOG_DIR / f"{process_name}.log"
        log_file = log_path.open("w")
        log_file.write(f"command: {' '.join(command)}\n\n")
        log_file.flush()
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            env=build_env(extra_env),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            # Tạo process group riêng để Ctrl+C có thể dừng cả Spark child process.
            start_new_session=True,
        )
        processes.append((process_name, process))

    return processes


def send_signal(process_name: str, process: subprocess.Popen, sig: int) -> None:
    """Gửi signal tới process group nếu process vẫn đang chạy."""
    if process.poll() is None:
        print(f"{process_name}: sending signal {sig}", flush=True)
        os.killpg(process.pid, sig)


def wait_for_exit(
    process_name: str,
    process: subprocess.Popen,
    timeout_seconds: int,
) -> bool:
    """Chờ process thoát trong timeout; trả về True nếu đã thoát."""
    if process.poll() is not None:
        return True

    try:
        process.wait(timeout=timeout_seconds)
        print(f"{process_name}: stopped", flush=True)
        return True
    except subprocess.TimeoutExpired:
        return False


def stop_one_process(process_name: str, process: subprocess.Popen) -> None:
    """Dừng một process group theo nhiều nấc để tránh live runner bị treo."""
    if process.poll() is not None:
        return

    # SIGINT giống Ctrl+C mềm, giúp Python/Spark có cơ hội chạy shutdown hook.
    send_signal(process_name, process, signal.SIGINT)
    if wait_for_exit(process_name, process, timeout_seconds=8):
        return

    # SIGTERM mạnh hơn, thường làm Spark in stack trace trước khi thoát.
    send_signal(process_name, process, signal.SIGTERM)
    if wait_for_exit(process_name, process, timeout_seconds=8):
        return

    # SIGKILL là nấc cuối để đảm bảo terminal không bị kẹt bởi Spark JVM.
    send_signal(process_name, process, signal.SIGKILL)
    process.wait(timeout=5)
    print(f"{process_name}: killed", flush=True)


def stop_processes(processes: list[tuple[str, subprocess.Popen]]) -> None:
    """Dừng toàn bộ process con của live pipeline."""
    for process_name, process in reversed(processes):
        stop_one_process(process_name, process)


def monitor_processes(processes: list[tuple[str, subprocess.Popen]]) -> None:
    """Giữ live pipeline chạy tới khi người dùng dừng hoặc một process lỗi."""
    print("\nLive pipeline is running. Press Ctrl+C to stop.\n", flush=True)

    while True:
        for process_name, process in processes:
            return_code = process.poll()
            if return_code is not None:
                # Live demo cần cả 3 process cùng sống; một process chết là pipeline lỗi.
                tail_log(process_name)
                raise SystemExit(
                    f"{process_name} exited unexpectedly with code {return_code}"
                )

        time.sleep(5)


def main() -> None:
    """Start live pipeline và dừng sạch khi nhận Ctrl+C."""
    processes = start_processes()

    try:
        monitor_processes(processes)
    except KeyboardInterrupt:
        print("\nStopping live pipeline...", flush=True)
    finally:
        stop_processes(processes)


if __name__ == "__main__":
    main()
