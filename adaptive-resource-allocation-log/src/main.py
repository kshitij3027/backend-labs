"""Eventlet bootstrap entry point for the Adaptive Resource Allocation System.

This module is the *process* entry point: it stands up the eventlet hub, wires the
control plane (:class:`~src.orchestrator.Orchestrator`) to the presentation plane
(:func:`~src.dashboard.create_app`), and runs the SocketIO server.

**Why ``eventlet.monkey_patch()`` is the very first thing.**
Eventlet replaces blocking stdlib primitives (``socket``, ``threading``, ``time``,
``select`` …) with cooperative green equivalents. For this to be correct, the patch
MUST run *before* any of those modules — or anything that imports them transitively
(flask, flask-socketio, psutil, the ``src`` modules) — is imported and binds the
original blocking implementations. So ``import eventlet`` / ``eventlet.monkey_patch()``
are the literal first two executable lines of this file, ahead of every other import.
This mirrors the canonical pattern in ``anomaly-detection-engine/src/app.py``.

**Why background loops are deferred with ``spawn_after``.**
:func:`~src.dashboard.start_background_tasks` launches green threads via
``socketio.start_background_task``. Those must be scheduled *after* the eventlet hub
is actually running, otherwise the loops can spin before the server is ready to
service them. :func:`main` therefore defers the launch with
``eventlet.spawn_after(1, start_background_tasks, …)`` and only then calls
``socketio.run(...)``, exactly as the sibling project does.

The server is started **only** under ``if __name__ == "__main__"`` so that importing
this module (e.g. from the test-suite, which calls :func:`build`) constructs the
objects without ever binding a socket or spinning a loop.
"""

import eventlet

eventlet.monkey_patch()

import logging  # noqa: E402  (imports must follow monkey_patch — see module docstring)
import signal  # noqa: E402
import sys  # noqa: E402

from src.config import Settings, load_config  # noqa: E402
from src.dashboard import create_app, start_background_tasks  # noqa: E402
from src.orchestrator import Orchestrator  # noqa: E402

logger = logging.getLogger(__name__)


def build() -> tuple:
    """Construct (but do not start) the full application object graph.

    This is the importable, side-effect-light core of the bootstrap: it loads
    configuration, wires the orchestrator (whose construction primes psutil's CPU
    counters via :class:`~src.metrics.MetricCollector`), and builds the Flask app
    plus its SocketIO server. It deliberately does **not** start the background
    loops or run the server, so tests can call it to assert the wiring is sound
    without standing up the eventlet server.

    Returns:
        A ``(app, socketio, orchestrator, config)`` tuple:
            * ``app`` — the configured :class:`flask.Flask` application.
            * ``socketio`` — its :class:`flask_socketio.SocketIO` server.
            * ``orchestrator`` — the wired :class:`~src.orchestrator.Orchestrator`.
            * ``config`` — the loaded :class:`~src.config.Settings`.
    """
    config: Settings = load_config()
    orchestrator = Orchestrator(config)
    app, socketio = create_app(config, orchestrator)
    return app, socketio, orchestrator, config


def main() -> None:
    """Boot the system: build the graph, defer the loops, and run the server.

    Steps:
        1. Configure root logging at the level from :class:`~src.config.Settings`.
        2. :func:`build` the application object graph.
        3. Install SIGINT/SIGTERM handlers that log and exit cleanly.
        4. Print the startup banner (exact spec text).
        5. Defer :func:`~src.dashboard.start_background_tasks` with
           ``eventlet.spawn_after(1, …)`` so the loops start once the hub is up.
        6. Hand control to ``socketio.run`` (blocks, serving the dashboard).
    """
    app, socketio, orchestrator, config = build()

    logging.basicConfig(
        level=getattr(logging, str(config.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    def _handle_shutdown(signum, _frame) -> None:
        """Log the signal and exit 0 so the container stops cleanly."""
        logger.info("Received signal %s — shutting down", signum)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    port = config.port or 8080
    print("🚀 Starting Adaptive Resource Allocation System")
    print("📊 Metrics collection active")
    print("🎯 Adaptive scaling enabled")
    print(f"🌐 Dashboard available at: http://localhost:{port}")

    # Defer the background loops until the eventlet hub is running. spawn_after(1, …)
    # schedules start_background_tasks one second after socketio.run() boots the hub,
    # so the green threads it spawns have a live hub to cooperate with.
    eventlet.spawn_after(1, start_background_tasks, socketio, app, orchestrator, config)

    socketio.run(app, host=config.host, port=port)


if __name__ == "__main__":
    # Guarded so that importing this module (which runs monkey_patch and build via
    # the test-suite) never binds a socket or starts the server.
    main()
