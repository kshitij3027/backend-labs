"""Integration smoke test for the eventlet bootstrap entry point (:mod:`src.main`).

Importing :mod:`src.main` runs ``eventlet.monkey_patch()`` at import time, which
would patch the *pytest* process's stdlib (``socket``/``threading``/``time`` …) and
contaminate every other test in the run. To keep the test process clean, the smoke
check is performed in a **subprocess**: a child Python interpreter imports the module,
calls :func:`src.main.build`, and asserts the wiring — the monkey-patching is then
confined to that throwaway process.

The subprocess inherits ``PYTHONPATH=/app`` from the Docker test image, so
``import src.main`` resolves. The check exercises :func:`~src.main.build` (which only
*constructs* the app graph — no socket is bound and no loop is started, because
``socketio.run`` lives behind the ``if __name__ == "__main__"`` guard) and confirms
the dashboard's core routes are registered and that :func:`~src.main.main` is callable.
"""

import subprocess
import sys


def test_main_imports_and_builds():
    script = (
        "import src.main as m\n"
        "app, socketio, orch, config = m.build()\n"
        "rules = {r.rule for r in app.url_map.iter_rules()}\n"
        "assert '/health' in rules and '/api/status' in rules and '/api/scaling' in rules\n"
        "assert callable(m.main)\n"
        "print('MAIN_SMOKE_OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "MAIN_SMOKE_OK" in result.stdout
