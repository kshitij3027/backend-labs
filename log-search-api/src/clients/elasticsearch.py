from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from elasticsearch import AsyncElasticsearch
from fastapi import Request

if TYPE_CHECKING:
    from src.config import Settings

logger = logging.getLogger(__name__)


def make_es_client(settings: "Settings") -> AsyncElasticsearch:
    return AsyncElasticsearch(
        hosts=[settings.ELASTICSEARCH_URL],
        request_timeout=10,
        max_retries=2,
        retry_on_timeout=True,
        verify_certs=False,
        maxsize=25,
    )


async def ping_es(es: AsyncElasticsearch) -> tuple[bool, str | None]:
    try:
        await asyncio.wait_for(es.cluster.health(), timeout=2.0)
        return True, None
    except Exception as exc:
        return False, str(exc)


def get_es(request: Request) -> AsyncElasticsearch:
    return request.app.state.es
