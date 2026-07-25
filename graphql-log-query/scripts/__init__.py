"""Black-box verification harnesses, run from the tester image against the LIVE stack.

Two modules land here later:

* ``verify_e2e.py`` (C12) — ordered black-box checks over HTTP + a real graphql-transport-ws
  socket, exit 1 on the first failure.
* ``load_test.py`` (C14) — concurrent perf/load phases with hard, host-overridable gates.

This ``__init__.py`` exists from **C1**, before either module does, and that is load-bearing in
two places: ``Dockerfile.test``'s ``COPY scripts/ ./scripts/`` needs the directory to exist at all
(a COPY of a missing path fails the build), and the compose services invoke them as
``python -m scripts.verify_e2e`` / ``python -m scripts.load_test``, which only resolves if
``scripts`` is a package on ``PYTHONPATH``. Creating the marker later would mean the compose file
had to change too.

They run as separate processes in a separate container and are **black-box on purpose**: they
reach the API by service name over the compose network (``http://api:8000``), never via a
published host port, so they are immune to an ``API_PORT`` collision on the host. They import from
``src`` only for ground truth (settings, the deterministic generator) — never to call a resolver
directly, because a harness that reaches past the transport stops measuring the thing that ships.
"""
