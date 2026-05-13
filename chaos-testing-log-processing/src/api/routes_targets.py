"""Inspect the allowlisted Docker targets."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from .dependencies import get_docker_client
from .schemas import TargetInfo

router = APIRouter(prefix="/targets", tags=["targets"])


@router.get("", response_model=list[TargetInfo])
async def list_targets(docker_client=Depends(get_docker_client)) -> list[TargetInfo]:
    out: list[TargetInfo] = []
    for c in docker_client.list_chaos_targets():
        out.append(
            TargetInfo(
                name=c.name,
                id=c.id,
                image=str(c.image),
                status=getattr(c, "status", None),
                labels={k: v for k, v in (c.labels or {}).items() if isinstance(k, str)},
            )
        )
    return out
