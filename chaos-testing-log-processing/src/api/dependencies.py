"""FastAPI dependency providers."""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    sm = request.app.state.db_sessionmaker
    async with sm() as session:
        yield session


def get_settings(request: Request):
    return request.app.state.settings


def get_docker_client(request: Request):
    return request.app.state.docker_client


def get_run_manager(request: Request):
    return request.app.state.run_manager


def get_injector(request: Request):
    return request.app.state.injector


def get_monitor(request: Request):
    return request.app.state.monitor
