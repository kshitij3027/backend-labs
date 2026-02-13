"""Local orchestrator — starts all long-lived pipeline components as subprocesses."""

import os
import signal
import subprocess
import sys
import time

PID_FILE = os.path.join(os.path.dirname(__file__), ".pipeline.pids")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

COMPONENTS = [
    ("generator", [sys.executable, "-m", "generator.main"]),
    ("collector", [sys.executable, "-m", "collector.main"]),
    ("parser", [sys.executable, "-m", "parser.main"]),
    ("storage", [sys.executable, "-m", "storage.main"]),
]


def start() -> None:
    if os.path.exists(PID_FILE):
        print("Pipeline appears to be running. Run 'stop' first.")
        sys.exit(1)

    env = os.environ.copy()
    env["CONFIG_PATH"] = os.path.join(BASE_DIR, "config.yml")
    env["PYTHONPATH"] = BASE_DIR

    # Create local dirs
    for d in ["logs", "data/collected", "data/parsed",
              "data/storage/active", "data/storage/archive", "data/storage/index"]:
        os.makedirs(os.path.join(BASE_DIR, d), exist_ok=True)

    pids: list[tuple[str, int]] = []
    procs: list[subprocess.Popen] = []

    for name, cmd in COMPONENTS:
        p = subprocess.Popen(cmd, cwd=BASE_DIR, env=env)
        pids.append((name, p.pid))
        procs.append(p)
        print(f"Started {name} (PID {p.pid})")
        time.sleep(0.5)

    with open(PID_FILE, "w") as f:
        for name, pid in pids:
            f.write(f"{name}:{pid}\n")

    print(f"\nPipeline running. PIDs saved to {PID_FILE}")
    print("Use 'python pipeline.py stop' to shut down.")

    # Wait for all children (or Ctrl+C)
    def _shutdown(signum, frame):
        print("\nShutting down pipeline...")
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            p.wait()
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        print("All components stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for p in procs:
        p.wait()

    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)


def stop() -> None:
    if not os.path.exists(PID_FILE):
        print("No PID file found. Pipeline may not be running.")
        return

    with open(PID_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            name, pid_str = line.split(":", 1)
            pid = int(pid_str)
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"Sent SIGTERM to {name} (PID {pid})")
            except ProcessLookupError:
                print(f"{name} (PID {pid}) already stopped")

    os.remove(PID_FILE)
    print("Pipeline stopped.")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("start", "stop"):
        print("Usage: python pipeline.py [start|stop]")
        sys.exit(1)

    if sys.argv[1] == "start":
        start()
    else:
        stop()
