from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import OAuth2PasswordBearer

from src.auth.security import decode_token
from src.config import Settings, get_settings

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl=f"{get_settings().API_V1_PREFIX}/auth/token",
    auto_error=True,
)


async def get_current_user(
    request: Request,
    token: Annotated[str, Depends(oauth2_scheme)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> str:
    payload = decode_token(token, settings)
    subject = payload.get("sub")
    if not isinstance(subject, str) or not subject:
        from fastapi import HTTPException, status

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    request.state.user_id = subject
    return subject


RequireUser = Annotated[str, Depends(get_current_user)]
