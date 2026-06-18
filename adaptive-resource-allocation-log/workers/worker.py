"""Trivial long-running "log processor" worker for the Docker worker backend.

This is the process the :class:`src.workers.DockerWorkerPool` spawns one container per
"worker". It does no real work — it just stays alive and prints a periodic heartbeat so
that the pool has a running, observable container to count and reconcile. A real system
would replace this loop with an actual log-consuming workload; here it only needs to
exist and keep running.
"""

import time

if __name__ == "__main__":
    # Print a heartbeat every 60s. The pool never reads this output; it exists only so
    # operators tailing container logs can see the worker is alive.
    while True:
        print("worker heartbeat", flush=True)
        time.sleep(60)
