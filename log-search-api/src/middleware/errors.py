from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request

logger = logging.getLogger(__name__)


_HTTP_CODE_MAP: dict[int, str] = {
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    409: "CONFLICT",
    415: "UNSUPPORTED_MEDIA_TYPE",
    422: "VALIDATION_ERROR",
    429: "RATE_LIMITED",
}


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _attach_request_id_header(response: ORJSONResponse, rid: str | None) -> ORJSONResponse:
    if rid:
        response.headers["X-Request-ID"] = rid
    return response


def _field_path(loc: tuple[Any, ...]) -> str:
    if not loc:
        return ""
    parts = [str(p) for p in loc[1:]] if len(loc) > 1 else [str(loc[0])]
    return ".".join(parts) if parts else str(loc[0])


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> ORJSONResponse:
    details: list[dict[str, Any]] = []
    fields_seen: list[str] = []
    for err in exc.errors():
        loc = tuple(err.get("loc", ()))
        field = _field_path(loc)
        message = err.get("msg", "invalid value")
        entry: dict[str, Any] = {"field": field, "message": message}
        err_type = err.get("type")
        if err_type:
            entry["type"] = err_type
        details.append(entry)
        if field and field not in fields_seen:
            fields_seen.append(field)

    suggestions = [f"check field '{name}'" for name in fields_seen] or [
        "review the request body and query parameters"
    ]

    rid = _request_id(request)
    body: dict[str, Any] = {
        "error": {
            "code": "VALIDATION_ERROR",
            "message": "request validation failed",
            "suggestions": suggestions,
        },
        "details": details,
        "request_id": rid,
    }
    return _attach_request_id_header(ORJSONResponse(status_code=422, content=body), rid)


async def http_exception_handler(
    request: Request, exc: HTTPException
) -> ORJSONResponse:
    code = _HTTP_CODE_MAP.get(exc.status_code, f"HTTP_{exc.status_code}")
    if isinstance(exc.detail, str):
        message = exc.detail
    elif exc.detail is None:
        message = "request failed"
    else:
        message = "request failed"

    rid = _request_id(request)
    body: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "suggestions": [],
        },
        "request_id": rid,
    }
    headers = dict(exc.headers) if exc.headers else None
    return _attach_request_id_header(
        ORJSONResponse(status_code=exc.status_code, content=body, headers=headers),
        rid,
    )


async def unhandled_exception_handler(
    request: Request, exc: Exception
) -> ORJSONResponse:
    rid = _request_id(request)
    logger.exception("unhandled_error", extra={"request_id": rid})
    body: dict[str, Any] = {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "unexpected server error",
            "suggestions": [
                "retry the request",
                "contact support with the request_id",
            ],
        },
        "request_id": rid,
    }
    return _attach_request_id_header(ORJSONResponse(status_code=500, content=body), rid)


async def rate_limit_exceeded_handler(
    request: Request, exc: RateLimitExceeded
) -> ORJSONResponse:
    detail = getattr(exc, "detail", None)
    message = (
        str(detail)
        if isinstance(detail, str) and detail
        else "rate limit exceeded"
    )
    rid = _request_id(request)
    body: dict[str, Any] = {
        "error": {
            "code": "RATE_LIMITED",
            "message": message,
            "suggestions": [
                "slow down request rate",
                "retry after the Retry-After header value",
            ],
        },
        "request_id": rid,
    }
    response = ORJSONResponse(status_code=429, content=body)
    if rid:
        response.headers["X-Request-ID"] = rid
    response.headers["X-RateLimit-Remaining"] = "0"
    limit = getattr(exc, "limit", None)
    if limit is not None:
        try:
            response.headers["X-RateLimit-Limit"] = str(limit.limit.amount)
        except Exception:
            pass
        try:
            response.headers["Retry-After"] = str(int(limit.limit.GRANULARITY.seconds))
        except Exception:
            pass
    return response


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
