"""Auth endpoints: POST /api/auth/login, GET /api/auth/profile."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from src.auth.dependencies import CurrentUser
from src.auth.service import AuthenticationError
from src.schemas.auth import LoginRequest, LoginResponse, UserInfo
from src.shared import auth_service as _auth_service  # noqa: E402

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest) -> LoginResponse:
    try:
        token, expires_at, user = _auth_service.login(payload.username, payload.password)
    except AuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )
    return LoginResponse(
        access_token=token,
        expires_at=expires_at,
        user_info=UserInfo(
            user_id=user.user_id,
            username=user.username,
            display_name=user.display_name,
            roles=list(user.roles),
        ),
    )


@router.get("/profile", response_model=UserInfo)
async def profile(user: CurrentUser) -> UserInfo:
    return UserInfo(
        user_id=user.user_id,
        username=user.username,
        display_name=user.display_name,
        roles=list(user.roles),
    )
