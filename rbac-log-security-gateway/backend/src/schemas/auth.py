"""Pydantic schemas for the /api/auth endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=80)
    password: str = Field(min_length=1, max_length=200)


class UserInfo(BaseModel):
    user_id: str
    username: str
    display_name: str = ""
    roles: List[str]


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime
    user_info: UserInfo
