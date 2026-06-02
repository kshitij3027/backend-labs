"""asyncpg connection-pool helpers for the L3 / slow-backend Postgres.

The rest of the system talks to Postgres through an :class:`asyncpg.Pool`
created here. ``DATABASE_URL`` is expected in the plain ``postgresql://`` form
(no SQLAlchemy ``+asyncpg`` suffix), but :func:`normalize_dsn` defends against a
SQLAlchemy-style DSN sneaking in.
"""
from __future__ import annotations

import pathlib
from typing import Union

import asyncpg

# Path to the schema DDL, resolved relative to this module so it works
# regardless of the process working directory (host or container).
_SCHEMA_PATH = pathlib.Path(__file__).with_name("schema.sql")


def normalize_dsn(url: str) -> str:
    """Strip a SQLAlchemy-style ``+driver`` suffix from a Postgres DSN.

    asyncpg only understands the plain scheme, so ``postgresql+asyncpg://...``
    must become ``postgresql://...``. A DSN that is already plain is returned
    unchanged.
    """
    scheme, sep, rest = url.partition("://")
    if not sep:
        # Not a URL we recognize; hand it back untouched.
        return url
    base_scheme = scheme.split("+", 1)[0]
    return f"{base_scheme}://{rest}"


async def create_pool(
    dsn: str,
    *,
    min_size: int = 1,
    max_size: int = 10,
) -> asyncpg.Pool:
    """Create an asyncpg connection pool against ``dsn``.

    The DSN is normalized first so a SQLAlchemy-style URL still works.
    """
    return await asyncpg.create_pool(
        normalize_dsn(dsn),
        min_size=min_size,
        max_size=max_size,
    )


async def apply_schema(pool_or_conn: Union[asyncpg.Pool, asyncpg.Connection]) -> None:
    """Apply ``schema.sql`` (idempotent DDL).

    Accepts either a :class:`asyncpg.Pool` (a connection is acquired) or an
    already-acquired :class:`asyncpg.Connection`. Safe to call repeatedly.
    """
    ddl = _SCHEMA_PATH.read_text(encoding="utf-8")
    if isinstance(pool_or_conn, asyncpg.Pool):
        async with pool_or_conn.acquire() as conn:
            await conn.execute(ddl)
    else:
        await pool_or_conn.execute(ddl)
