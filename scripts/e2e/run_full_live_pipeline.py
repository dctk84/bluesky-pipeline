"""Chạy full live pipeline gồm fast path và Gold incremental path."""

from __future__ import annotations

import os
import queue
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.e2e.run_live_pipeline import (
    LOG_DIR,
    LOG_TAIL_LINES,
    build_env,
    start_processes,
    stop_processes,
    tail_log,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_INCREMENTAL_LOG_PATH = LOG_DIR / "gold-incremental.log"
GOLD_INCREMENTAL_MODULE = "scripts.lakehouse.run_lakehouse_path_incremental"


def parse_positive_int_env(name: str, default_value: int) -> int:
    """Đọc biến môi trường dạng số nguyên dương.

    Input là tên biến môi trường và giá trị mặc định.
    Output là số nguyên dương dùng để cấu hình interval local runner.
    """
    raw_value = os.getenv(name)

    if raw_value is None:
        return default_value

    parsed_value = int(raw_value)

    if parsed_value <= 0:
        raise ValueError(f"{name} must be > 0")

    return parsed_value


def parse_bool_env(name: str, default_value: bool = False) -> bool:
    """Đọc biến môi trường dạng boolean đơn giản.

    Input là tên biến môi trường và giá trị mặc định.
    Output là True nếu giá trị thuộc nhóm bật.
    """
    raw_value = os.getenv(name)

    if raw_value is None:
        return default_value

    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def append_gold_log(message: str) -> None:
    """Ghi một dòng log cho Gold incremental worker."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with GOLD_INCREMENTAL_LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(message + "\n")
        log_file.flush()


def tail_gold_incremental_log() -> None:
    """In các dòng cuối của Gold incremental log để debug khi job lỗi."""
    if not GOLD_INCREMENTAL_LOG_PATH.exists():
        print(
            f"gold-incremental: log file not found: {GOLD_INCREMENTAL_LOG_PATH}",
            flush=True,
        )
        return

    lines = GOLD_INCREMENTAL_LOG_PATH.read_text(errors="replace").splitlines()
    print(
        f"\n--- gold-incremental log tail ({GOLD_INCREMENTAL_LOG_PATH}) ---",
        flush=True,
    )
    for line in lines[-LOG_TAIL_LINES:]:
        print(line, flush=True)
    print("--- end gold-incremental log tail ---\n", flush=True)


def stop_gold_subprocess(process: subprocess.Popen) -> None:
    """Dừng Gold incremental subprocess theo nhiều nấc.

    Input là process đang chạy.
    Output là process đã thoát hoặc bị kill.
    """
    if process.poll() is not None:
        return

    for sig, timeout_seconds in [
        (signal.SIGINT, 8),
        (signal.SIGTERM, 8),
        (signal.SIGKILL, 5),
    ]:
        if process.poll() is not None:
            return

        os.killpg(process.pid, sig)

        try:
            process.wait(timeout=timeout_seconds)
            return
        except subprocess.TimeoutExpired:
            continue


def run_gold_incremental_once(
    run_number: int,
    ignore_state: bool,
    stop_event: threading.Event,
) -> None:
    """Chạy một lần lakehouse Gold incremental path.

    Input là số thứ tự lần chạy và flag có truyền `--ignore-state` hay không.
    Output là process con hoàn tất thành công hoặc raise nếu pipeline lỗi.
    """
    command = [sys.executable, "-m", GOLD_INCREMENTAL_MODULE]

    # Full live runner chạy trên dữ liệu đang thay đổi, nên dùng readiness check.
    command.append("--live-mode")

    if ignore_state:
        command.append("--ignore-state")

    started_at = datetime.now(timezone.utc).isoformat()
    append_gold_log("")
    append_gold_log(f"=== gold incremental run {run_number} started_at={started_at} ===")
    append_gold_log(f"command: {' '.join(command)}")

    with GOLD_INCREMENTAL_LOG_PATH.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            env=build_env({}),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        while process.poll() is None:
            if stop_event.wait(1):
                stop_gold_subprocess(process)
                return

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)

    finished_at = datetime.now(timezone.utc).isoformat()
    append_gold_log(f"=== gold incremental run {run_number} finished_at={finished_at} ===")


def gold_incremental_loop(
    stop_event: threading.Event,
    error_queue: queue.Queue[BaseException],
) -> None:
    """Chạy Gold incremental định kỳ cho full live runner.

    Input là stop event và queue báo lỗi về main thread.
    Output là loop kết thúc khi nhận stop event hoặc đẩy exception vào queue.
    """
    initial_delay_seconds = parse_positive_int_env(
        "GOLD_INCREMENTAL_INITIAL_DELAY_SECONDS",
        120,
    )
    interval_seconds = parse_positive_int_env(
        "GOLD_INCREMENTAL_INTERVAL_SECONDS",
        300,
    )
    ignore_state_first_run = parse_bool_env(
        "GOLD_INCREMENTAL_IGNORE_STATE_FIRST_RUN",
        False,
    )

    append_gold_log(
        "gold incremental worker configured: "
        f"initial_delay_seconds={initial_delay_seconds}, "
        f"interval_seconds={interval_seconds}, "
        f"ignore_state_first_run={ignore_state_first_run}"
    )

    if stop_event.wait(initial_delay_seconds):
        return

    run_number = 1

    while not stop_event.is_set():
        try:
            run_gold_incremental_once(
                run_number,
                ignore_state=ignore_state_first_run and run_number == 1,
                stop_event=stop_event,
            )
        except BaseException as error:
            error_queue.put(error)
            return

        run_number += 1

        if stop_event.wait(interval_seconds):
            return


def start_gold_incremental_worker() -> tuple[threading.Event, queue.Queue, threading.Thread]:
    """Start thread chạy Gold incremental định kỳ.

    Output gồm stop event, error queue và worker thread.
    """
    stop_event = threading.Event()
    error_queue: queue.Queue[BaseException] = queue.Queue()
    worker = threading.Thread(
        target=gold_incremental_loop,
        args=(stop_event, error_queue),
        name="gold-incremental-worker",
        daemon=True,
    )
    worker.start()

    return stop_event, error_queue, worker


def stop_gold_incremental_worker(
    stop_event: threading.Event,
    worker: threading.Thread,
) -> None:
    """Yêu cầu Gold incremental worker dừng và chờ thread kết thúc."""
    stop_event.set()
    worker.join(timeout=5)

    if worker.is_alive():
        print(
            "gold-incremental: worker is still running; "
            "current subprocess will finish or be killed with parent process",
            flush=True,
        )


def monitor_full_pipeline(
    processes: list[tuple[str, subprocess.Popen]],
    gold_error_queue: queue.Queue,
) -> None:
    """Giữ full live pipeline chạy và fail-fast nếu process/job lỗi."""
    print(
        "\nFull live pipeline is running. Press Ctrl+C to stop.\n",
        flush=True,
    )
    print(
        "Gold incremental worker log: "
        f"{GOLD_INCREMENTAL_LOG_PATH}",
        flush=True,
    )

    while True:
        for process_name, process in processes:
            return_code = process.poll()

            if return_code is not None:
                tail_log(process_name)
                raise SystemExit(
                    f"{process_name} exited unexpectedly with code {return_code}"
                )

        try:
            gold_error = gold_error_queue.get_nowait()
        except queue.Empty:
            pass
        else:
            tail_gold_incremental_log()
            raise SystemExit(
                "gold-incremental exited unexpectedly. "
                f"error={gold_error!r}"
            ) from gold_error

        time.sleep(5)


def main() -> None:
    """Start full live pipeline và dừng sạch khi nhận Ctrl+C."""
    processes = start_processes()
    gold_stop_event, gold_error_queue, gold_worker = start_gold_incremental_worker()

    try:
        monitor_full_pipeline(processes, gold_error_queue)
    except KeyboardInterrupt:
        print("\nStopping full live pipeline...", flush=True)
    finally:
        stop_gold_incremental_worker(gold_stop_event, gold_worker)
        stop_processes(processes)


if __name__ == "__main__":
    # SIGINT được xử lý ở main để dừng cả live processes và Gold worker.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    main()
