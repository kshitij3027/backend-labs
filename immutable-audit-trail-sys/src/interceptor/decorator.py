"""@audit_access decorator — drop-in interception of log-access calls.

Captures (actor, action, resource, args_digest, result_digest, processing_ms,
success, error_message) on every call and forwards them to a process-wide
ChainAppender. Detects async vs sync at decoration time.

Two design choices worth noting:

1. **Registry over dependency injection.** The decorator pulls the
   ChainAppender from a module-level registry set during FastAPI lifespan
   (`set_appender(app.state.appender)`). The alternative — passing the
   appender to the decorator at call time — would force every wrapped
   function to take an extra argument. The registry trades a bit of
   global state for ergonomics; production code keeps it confined to
   exactly one set-call during lifespan.

2. **Audit failures never propagate.** A wrapped function's outcome is
   the source of truth. If the audit write itself blows up, we log a
   warning and return the wrapped result (or re-raise the wrapped
   exception). This is the "audit observability" principle — instrumentation
   must not change the behaviour it's observing.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from typing import Any, Callable, Optional

from src.chain.appender import ChainAppender
from src.chain.schema import args_digest, result_digest
from src.settings import get_settings

log = logging.getLogger(__name__)


# --- Process-wide registry --------------------------------------------------

_APPENDER: Optional[ChainAppender] = None


def set_appender(appender: ChainAppender) -> None:
    """Install the process-wide appender. Called once during FastAPI lifespan."""
    global _APPENDER
    _APPENDER = appender


def get_appender() -> Optional[ChainAppender]:
    """Read the registered appender. Returns None if unset (test scenarios)."""
    return _APPENDER


def clear_appender() -> None:
    """Clear the registry. Useful for tests."""
    global _APPENDER
    _APPENDER = None


# --- Internal helpers ------------------------------------------------------

def _resolve_attr_path(obj: Any, path: str) -> Any:
    """Walk a dotted attribute path on ``obj``. Returns the value or repr if missing."""
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = getattr(cur, part, None)
        if cur is None:
            return f"<missing:{path}>"
    return cur


def _extract_actor(args: tuple, kwargs: dict[str, Any]) -> str:
    """Pull X-User-ID from a Request kwarg or args; else anonymous fallback."""
    settings = get_settings()
    header = settings.user_header_name
    anon = settings.anonymous_user_id

    # Look in kwargs first.
    for v in kwargs.values():
        actor = _maybe_actor_from(v, header)
        if actor is not None:
            return actor
    # Then args.
    for v in args:
        actor = _maybe_actor_from(v, header)
        if actor is not None:
            return actor
    return anon


def _maybe_actor_from(v: Any, header: str) -> Optional[str]:
    """If ``v`` is a Request-ish object with headers, pull the header value."""
    # Avoid importing fastapi.Request at module load time — duck-type instead.
    headers = getattr(v, "headers", None)
    if headers is None:
        return None
    try:
        actor = headers.get(header)
    except Exception:  # pragma: no cover - defensive
        return None
    if actor and isinstance(actor, str):
        return actor
    return None


def _extract_resource(
    args: tuple,
    kwargs: dict[str, Any],
    resource_from: Optional[str],
    resource_static: Optional[str],
) -> str:
    if resource_static is not None:
        return resource_static
    if resource_from is None:
        return "<unspecified>"
    # resource_from may be a top-level kwarg name ("query") or a dotted
    # path on the first non-self positional arg ("query.target").
    head, _, rest = resource_from.partition(".")
    if head in kwargs:
        return str(_resolve_attr_path(kwargs[head], rest) if rest else kwargs[head])
    # Fall back to args[0] for the head's value (common decorator pattern
    # where the first arg is the call's payload).
    if args:
        return str(_resolve_attr_path(args[0], rest) if rest else args[0])
    return f"<missing:{resource_from}>"


async def _safe_append(
    *,
    actor: str,
    action: str,
    resource: str,
    success: bool,
    args_d: str,
    result_d: str,
    processing_ms: float,
    error_message: Optional[str],
) -> None:
    """Call ChainAppender.append. Swallow any audit-side error."""
    from src.stats.counters import get_counters
    get_counters().incr_decorator_invocations()
    appender = get_appender()
    if appender is None:
        log.warning("audit_access invoked but no appender registered")
        return
    try:
        await appender.append(
            actor=actor,
            action=action,
            resource=resource,
            success=success,
            args_digest=args_d,
            result_digest=result_d,
            processing_ms=processing_ms,
            error_message=error_message,
        )
    except Exception as exc:  # noqa: BLE001 — intentional broad catch
        log.warning("audit append failed: %s", exc, exc_info=True)


# --- Public decorator ------------------------------------------------------

def audit_access(
    *,
    action: str,
    resource_from: Optional[str] = None,
    resource_static: Optional[str] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator factory: wraps a sync or async callable with audit recording.

    Args:
        action: short tag like "read", "search", "export" — fed verbatim
            into the audit record.
        resource_from: dotted attribute path identifying the resource on
            one of the wrapped function's args (e.g. "query.target" pulls
            ``query.target`` from the ``query`` kwarg).
        resource_static: literal string used as the resource when the call
            doesn't carry a natural identifier (mutually exclusive with
            resource_from; if both are given, resource_static wins).
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        is_coro = inspect.iscoroutinefunction(fn)

        if is_coro:
            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                actor = _extract_actor(args, kwargs)
                resource = _extract_resource(args, kwargs, resource_from, resource_static)
                a_dig = args_digest(args, kwargs)
                try:
                    result = await fn(*args, **kwargs)
                except Exception as exc:
                    processing_ms = (time.perf_counter() - start) * 1000.0
                    from src.stats.counters import get_counters
                    get_counters().incr_decorator_failures()
                    await _safe_append(
                        actor=actor, action=action, resource=resource,
                        success=False, args_d=a_dig, result_d="",
                        processing_ms=processing_ms,
                        error_message=str(exc),
                    )
                    raise
                processing_ms = (time.perf_counter() - start) * 1000.0
                r_dig = result_digest(result)
                await _safe_append(
                    actor=actor, action=action, resource=resource,
                    success=True, args_d=a_dig, result_d=r_dig,
                    processing_ms=processing_ms,
                    error_message=None,
                )
                from src.stats.counters import get_counters
                get_counters().observe_decorator_overhead_ms(processing_ms)
                return result

            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            actor = _extract_actor(args, kwargs)
            resource = _extract_resource(args, kwargs, resource_from, resource_static)
            a_dig = args_digest(args, kwargs)
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                processing_ms = (time.perf_counter() - start) * 1000.0
                from src.stats.counters import get_counters
                get_counters().incr_decorator_failures()
                # Sync caller can still record via asyncio.run; if we're
                # already inside a running loop, schedule via a task.
                _schedule_audit(
                    actor=actor, action=action, resource=resource,
                    success=False, args_d=a_dig, result_d="",
                    processing_ms=processing_ms,
                    error_message=str(exc),
                )
                raise
            processing_ms = (time.perf_counter() - start) * 1000.0
            r_dig = result_digest(result)
            _schedule_audit(
                actor=actor, action=action, resource=resource,
                success=True, args_d=a_dig, result_d=r_dig,
                processing_ms=processing_ms,
                error_message=None,
            )
            from src.stats.counters import get_counters
            get_counters().observe_decorator_overhead_ms(processing_ms)
            return result

        return sync_wrapper

    return decorator


def _schedule_audit(**kwargs: Any) -> None:
    """Schedule an async _safe_append from a sync context.

    If a loop is already running, fire-and-forget as a task. Otherwise
    spin up a one-shot loop. Either way, audit recording stays fully
    out of the wrapped function's critical path.
    """
    coro = _safe_append(**kwargs)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        # No running loop — synchronous context. asyncio.run handles cleanup.
        asyncio.run(coro)
