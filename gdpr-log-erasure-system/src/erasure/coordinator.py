"""Erasure coordinator — drives the full request lifecycle.

States: PENDING → DISCOVERING → EXECUTING → VERIFYING → COMPLETED
        (any state → FAILED on error).

Per-location work parallelised via asyncio.gather + semaphore.
Every state transition + per-location action is appended to the audit chain.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.audit.chain import append_audit_entry
from src.erasure.anonymization import decide_action
from src.erasure.executor import erase_location
from src.erasure.state_machine import assert_transition
from src.erasure.verifier import verify_erasure
from src.logging_config import get_logger
from src.persistence.models import (
    ErasureRequest, RequestState, RequestType, UserDataMapping,
)
from src.settings import Settings


log = get_logger(__name__)


class ErasureCoordinator:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._sf = session_factory
        self._settings = settings

    # ── public entrypoint ─────────────────────────────────────────────────

    async def process(self, request_id: str) -> None:
        """Drive a single erasure request through its full lifecycle."""
        try:
            mappings = await self._discover(request_id)
            results = await self._execute(request_id, mappings)
            failures = [r for r in results if r["result"] != "ok"]
            if failures:
                await self._fail(request_id, f"{len(failures)} location(s) failed", failures)
                return
            if self._settings.verification_enabled:
                ok = await self._verify(request_id, results)
                if not ok:
                    await self._fail(request_id, "verification step failed", results)
                    return
            await self._complete(request_id, results)
        except Exception as e:  # safety net — never raise out of a BackgroundTask
            log.error("coordinator.exception", request_id=request_id, error=repr(e))
            try:
                await self._fail(request_id, f"unhandled: {e!r}", [])
            except Exception:
                log.error("coordinator.fail_log_failed", request_id=request_id)

    # ── phase helpers ─────────────────────────────────────────────────────

    async def _discover(self, request_id: str) -> list[UserDataMapping]:
        async with self._sf() as session:
            req = await self._load_request(session, request_id)
            assert_transition(req.state, RequestState.DISCOVERING)
            req.state = RequestState.DISCOVERING
            req.started_at = _utcnow()
            session.add(req)
            await append_audit_entry(
                session,
                request_id=request_id,
                event_type="STATE_TRANSITION",
                payload={"from": RequestState.PENDING.value, "to": RequestState.DISCOVERING.value},
            )
            await session.commit()

            mappings = (await session.execute(
                select(UserDataMapping).where(UserDataMapping.user_id == req.user_id)
            )).scalars().all()
            await append_audit_entry(
                session,
                request_id=request_id,
                event_type="DISCOVERY_COMPLETE",
                payload={"locations_found": len(mappings)},
            )
            await session.commit()
            return list(mappings)

    async def _execute(
        self,
        request_id: str,
        mappings: list[UserDataMapping],
    ) -> list[dict[str, Any]]:
        async with self._sf() as session:
            req = await self._load_request(session, request_id)
            assert_transition(req.state, RequestState.EXECUTING)
            req.state = RequestState.EXECUTING
            session.add(req)
            await append_audit_entry(
                session,
                request_id=request_id,
                event_type="STATE_TRANSITION",
                payload={"from": RequestState.DISCOVERING.value, "to": RequestState.EXECUTING.value},
            )
            await session.commit()
            request_type = req.request_type.value

        if not mappings:
            return []

        sem = asyncio.Semaphore(self._settings.max_parallel_location_erasures)
        allowlist = self._settings.anonymizable_data_types_set
        salt = self._settings.anonymization_hash_salt
        retry_count = self._settings.erasure_retry_count
        retry_backoff = self._settings.erasure_retry_backoff_seconds

        async def _one(m: UserDataMapping) -> dict[str, Any]:
            action = decide_action(request_type, m.data_type, allowlist)
            async with sem:
                async with self._sf() as session:
                    # re-attach: re-fetch the row in this session
                    fresh = (await session.execute(
                        select(UserDataMapping).where(UserDataMapping.id == m.id)
                    )).scalar_one_or_none()
                    if fresh is None:
                        return {
                            "mapping_id": m.id, "action": action,
                            "result": "failed", "error": "mapping disappeared mid-flight",
                            "data_type": m.data_type, "storage_location": m.storage_location,
                        }
                    result = await erase_location(
                        session, mapping=fresh, action=action,
                        salt=salt,
                        retry_count=retry_count,
                        retry_backoff_seconds=retry_backoff,
                    )
                    await append_audit_entry(
                        session,
                        request_id=request_id,
                        event_type="LOCATION_ERASED" if result["result"] == "ok" else "LOCATION_FAILED",
                        payload=result,
                    )
                    await session.commit()
                    return result

        return await asyncio.gather(*[_one(m) for m in mappings])

    async def _verify(self, request_id: str, results: list[dict[str, Any]]) -> bool:
        async with self._sf() as session:
            req = await self._load_request(session, request_id)
            assert_transition(req.state, RequestState.VERIFYING)
            req.state = RequestState.VERIFYING
            session.add(req)
            await append_audit_entry(
                session,
                request_id=request_id,
                event_type="STATE_TRANSITION",
                payload={"from": RequestState.EXECUTING.value, "to": RequestState.VERIFYING.value},
            )
            await session.commit()

            all_ok = True
            for r in results:
                ok = await verify_erasure(
                    session, mapping_id=r["mapping_id"], action=r["action"],
                )
                await append_audit_entry(
                    session,
                    request_id=request_id,
                    event_type="VERIFICATION_OK" if ok else "VERIFICATION_FAILED",
                    payload={"mapping_id": r["mapping_id"], "action": r["action"], "ok": ok},
                )
                if not ok:
                    all_ok = False
            await session.commit()
            return all_ok

    async def _complete(self, request_id: str, results: list[dict[str, Any]]) -> None:
        async with self._sf() as session:
            req = await self._load_request(session, request_id)
            assert_transition(req.state, RequestState.COMPLETED)
            req.state = RequestState.COMPLETED
            req.completed_at = _utcnow()
            session.add(req)
            await append_audit_entry(
                session,
                request_id=request_id,
                event_type="STATE_TRANSITION",
                payload={
                    "from": (RequestState.VERIFYING if self._settings.verification_enabled else RequestState.EXECUTING).value,
                    "to": RequestState.COMPLETED.value,
                    "locations_processed": len(results),
                },
            )
            await session.commit()

    async def _fail(
        self,
        request_id: str,
        error_message: str,
        results: list[dict[str, Any]],
    ) -> None:
        async with self._sf() as session:
            req = await self._load_request(session, request_id)
            previous = req.state
            req.state = RequestState.FAILED
            req.error_message = error_message
            req.completed_at = _utcnow()
            session.add(req)
            await append_audit_entry(
                session,
                request_id=request_id,
                event_type="STATE_TRANSITION",
                payload={
                    "from": previous.value, "to": RequestState.FAILED.value,
                    "error": error_message,
                    "failed_locations": [r for r in results if r.get("result") != "ok"],
                },
            )
            await session.commit()

    async def _load_request(self, session: AsyncSession, request_id: str) -> ErasureRequest:
        req = (await session.execute(
            select(ErasureRequest).where(ErasureRequest.id == request_id)
        )).scalar_one_or_none()
        if req is None:
            raise LookupError(f"erasure request not found: {request_id}")
        return req


def _utcnow() -> dt.datetime:
    return dt.datetime.utcnow().replace(microsecond=0)
