from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from src.auth.dependencies import RequireUser
from src.auth.security import create_access_token
from src.auth.users import SeededUserStore, get_user_store
from src.config import Settings, get_settings
from src.schemas.auth import TokenResponse, UserPublic

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


def _get_user_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> SeededUserStore:
    return get_user_store(settings)


@router.post("/token", response_model=TokenResponse)
async def issue_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    settings: Annotated[Settings, Depends(get_settings)],
    user_store: Annotated[SeededUserStore, Depends(_get_user_store)],
) -> TokenResponse:
    if not user_store.authenticate(form_data.username, form_data.password):
        logger.info("authentication failed for username=%s", form_data.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token, expires_at = create_access_token(form_data.username, settings)
    return TokenResponse(access_token=token, expires_at=expires_at)


@router.get("/me", response_model=UserPublic)
async def read_me(current_user: RequireUser) -> UserPublic:
    return UserPublic(username=current_user)
