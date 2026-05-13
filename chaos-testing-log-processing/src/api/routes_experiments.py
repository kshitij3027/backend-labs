"""CRUD endpoints for experiment definitions."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.experiments import ExperimentDefinition
from ..persistence.repo import ExperimentDefinitionRepo
from .dependencies import get_db
from .schemas import CreateExperimentRequest

router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.post("", response_model=ExperimentDefinition, status_code=status.HTTP_201_CREATED)
async def create_experiment(
    payload: CreateExperimentRequest,
    session: AsyncSession = Depends(get_db),
) -> ExperimentDefinition:
    definition = ExperimentDefinition(
        name=payload.name,
        description=payload.description,
        type=payload.type,
        target=payload.target,
        parameters=dict(payload.parameters),
        duration=payload.duration,
        severity=payload.severity,
        hypothesis=payload.hypothesis,
    )
    repo = ExperimentDefinitionRepo(session)
    await repo.create(definition)
    return definition


@router.get("", response_model=list[ExperimentDefinition])
async def list_experiments(
    target: str | None = None,
    type_: str | None = None,
    limit: int = 50,
    session: AsyncSession = Depends(get_db),
) -> list[ExperimentDefinition]:
    repo = ExperimentDefinitionRepo(session)
    return await repo.list_all(target=target, type_=type_, limit=limit)


@router.get("/{experiment_id}", response_model=ExperimentDefinition)
async def get_experiment(
    experiment_id: str,
    session: AsyncSession = Depends(get_db),
) -> ExperimentDefinition:
    repo = ExperimentDefinitionRepo(session)
    found = await repo.get(experiment_id)
    if found is None:
        raise HTTPException(status_code=404, detail="experiment not found")
    return found
