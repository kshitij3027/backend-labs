"""Shared pytest configuration and fixtures."""

import os

import pytest
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base


def pytest_sessionfinish(session, exitstatus):
    """Treat 'no tests collected' (exit code 5) as success."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture(scope="session")
def database_url():
    return os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://alertuser:alertpass@postgres:5432/alertdb",
    )


@pytest.fixture(scope="session")
def redis_url():
    return os.environ.get("REDIS_URL", "redis://redis:6379/0")


@pytest.fixture
async def async_engine(database_url):
    """Create an async SQLAlchemy engine and set up / tear down tables."""
    engine = create_async_engine(database_url, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.fixture
async def db_session(async_engine):
    """Provide an async session for each test, rolling back afterwards."""
    async_session_factory = sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session_factory() as session:
        yield session


@pytest.fixture
async def redis_client(redis_url):
    """Provide a Redis client, flushing the DB before and after each test."""
    client = aioredis.from_url(redis_url, decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()
