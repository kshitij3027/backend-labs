"""Alert lifecycle REST API endpoints."""

from datetime import datetime
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.models import Alert, AlertRule, AlertState
from src.schemas import AcknowledgeRequest, AlertResponse, StatsResponse

router = APIRouter()
logger = structlog.get_logger(__name__)


@router.get("/alerts", response_model=list[AlertResponse])
async def list_alerts(
    state: Optional[str] = Query(None, description="Filter by alert state"),
    db: AsyncSession = Depends(get_db),
):
    """List all alerts, optionally filtered by state, sorted by last_occurrence desc."""
    query = select(Alert).order_by(Alert.last_occurrence.desc())

    if state is not None:
        # Validate that the state value is a known AlertState
        upper_state = state.upper()
        if upper_state not in AlertState.__members__:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid state '{state}'. Valid states: {[s.value for s in AlertState]}",
            )
        query = query.where(Alert.state == upper_state)

    result = await db.execute(query)
    alerts = result.scalars().all()
    logger.info("listed_alerts", count=len(alerts), state_filter=state)
    return alerts


@router.get("/alerts/{alert_id}", response_model=AlertResponse)
async def get_alert(
    alert_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a single alert by ID."""
    result = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = result.scalar_one_or_none()

    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    logger.info("fetched_alert", alert_id=alert_id)
    return alert


@router.post("/alerts/{alert_id}/acknowledge", response_model=AlertResponse)
async def acknowledge_alert(
    alert_id: int,
    body: AcknowledgeRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Acknowledge an alert. Sets state to ACKNOWLEDGED with acknowledger info."""
    result = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = result.scalar_one_or_none()

    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    if alert.state == AlertState.RESOLVED.value:
        raise HTTPException(
            status_code=400,
            detail="Cannot acknowledge a resolved alert",
        )

    alert.state = AlertState.ACKNOWLEDGED.value
    alert.acknowledged_by = body.acknowledged_by
    alert.acknowledged_at = datetime.utcnow()

    await db.commit()
    await db.refresh(alert)

    logger.info(
        "alert_acknowledged",
        alert_id=alert_id,
        acknowledged_by=body.acknowledged_by,
    )

    # Broadcast alert update via WebSocket
    connection_manager = request.app.state.connection_manager
    await connection_manager.broadcast_json({
        "type": "alert_update",
        "alert": {
            "id": alert.id,
            "pattern_name": alert.pattern_name,
            "severity": alert.severity,
            "message": alert.message,
            "count": alert.count,
            "state": alert.state,
            "first_occurrence": (
                alert.first_occurrence.isoformat()
                if alert.first_occurrence else None
            ),
            "last_occurrence": (
                alert.last_occurrence.isoformat()
                if alert.last_occurrence else None
            ),
        },
    })

    # Broadcast updated stats
    active_result = await db.execute(
        select(func.count(Alert.id)).where(
            Alert.state != AlertState.RESOLVED.value
        )
    )
    active_alerts = active_result.scalar() or 0
    patterns_result = await db.execute(
        select(func.count(AlertRule.id)).where(AlertRule.enabled.is_(True))
    )
    total_patterns = patterns_result.scalar() or 0
    severity_result = await db.execute(
        select(Alert.severity, func.count(Alert.id))
        .where(Alert.state != AlertState.RESOLVED.value)
        .group_by(Alert.severity)
    )
    alerts_by_severity = {row[0]: row[1] for row in severity_result.all()}
    await connection_manager.broadcast_json({
        "type": "stats_update",
        "stats": {
            "active_alerts": active_alerts,
            "total_patterns": total_patterns,
            "alerts_by_severity": alerts_by_severity,
        },
    })

    return alert


@router.post("/alerts/{alert_id}/resolve", response_model=AlertResponse)
async def resolve_alert(
    alert_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Resolve an alert. Sets state to RESOLVED with resolved_at timestamp."""
    result = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = result.scalar_one_or_none()

    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    if alert.state == AlertState.RESOLVED.value:
        raise HTTPException(
            status_code=400,
            detail="Alert is already resolved",
        )

    alert.state = AlertState.RESOLVED.value
    alert.resolved_at = datetime.utcnow()

    await db.commit()
    await db.refresh(alert)

    logger.info("alert_resolved", alert_id=alert_id)

    # Broadcast alert update via WebSocket
    connection_manager = request.app.state.connection_manager
    await connection_manager.broadcast_json({
        "type": "alert_update",
        "alert": {
            "id": alert.id,
            "pattern_name": alert.pattern_name,
            "severity": alert.severity,
            "message": alert.message,
            "count": alert.count,
            "state": alert.state,
            "first_occurrence": (
                alert.first_occurrence.isoformat()
                if alert.first_occurrence else None
            ),
            "last_occurrence": (
                alert.last_occurrence.isoformat()
                if alert.last_occurrence else None
            ),
        },
    })

    # Broadcast updated stats
    active_result = await db.execute(
        select(func.count(Alert.id)).where(
            Alert.state != AlertState.RESOLVED.value
        )
    )
    active_alerts = active_result.scalar() or 0
    patterns_result = await db.execute(
        select(func.count(AlertRule.id)).where(AlertRule.enabled.is_(True))
    )
    total_patterns = patterns_result.scalar() or 0
    severity_result = await db.execute(
        select(Alert.severity, func.count(Alert.id))
        .where(Alert.state != AlertState.RESOLVED.value)
        .group_by(Alert.severity)
    )
    alerts_by_severity = {row[0]: row[1] for row in severity_result.all()}
    await connection_manager.broadcast_json({
        "type": "stats_update",
        "stats": {
            "active_alerts": active_alerts,
            "total_patterns": total_patterns,
            "alerts_by_severity": alerts_by_severity,
        },
    })

    return alert


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    db: AsyncSession = Depends(get_db),
):
    """Return summary statistics for active alerts and enabled patterns."""
    # Count active alerts (state != RESOLVED)
    active_result = await db.execute(
        select(func.count(Alert.id)).where(
            Alert.state != AlertState.RESOLVED.value
        )
    )
    active_alerts = active_result.scalar() or 0

    # Count enabled alert rules
    patterns_result = await db.execute(
        select(func.count(AlertRule.id)).where(AlertRule.enabled.is_(True))
    )
    total_patterns = patterns_result.scalar() or 0

    # Group active alerts by severity
    severity_result = await db.execute(
        select(Alert.severity, func.count(Alert.id))
        .where(Alert.state != AlertState.RESOLVED.value)
        .group_by(Alert.severity)
    )
    alerts_by_severity = {row[0]: row[1] for row in severity_result.all()}

    logger.info(
        "fetched_stats",
        active_alerts=active_alerts,
        total_patterns=total_patterns,
    )
    return StatsResponse(
        active_alerts=active_alerts,
        total_patterns=total_patterns,
        alerts_by_severity=alerts_by_severity,
    )
