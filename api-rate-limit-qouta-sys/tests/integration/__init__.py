"""Integration suite — everything that needs a REAL redis:7-alpine over the compose network.

Separated from ``tests/unit/`` because the two answer different questions. A unit test proves the
arithmetic and the state machines; an integration test proves the assumptions about the *server* —
that ``register_script`` really does recover from a ``NOSCRIPT``, that a socket timeout really is
250 ms and not the kernel's default, that the Lua script's ``TIME``, integer coercion and RESP
encoding behave exactly as the decision logic assumes (C4).

``fakeredis[lua]`` is deliberately NOT the oracle for any of it: it is a reimplementation, and its
``TIME``, float coercion and Lua->RESP rules are approximations — precisely the wrong thing to
assert exactness against.

Run alone with ``make test-int``; included in ``make test``.
"""
