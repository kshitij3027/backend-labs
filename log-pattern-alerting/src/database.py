import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config import get_settings
from src.models import Base

logger = logging.getLogger(__name__)

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db():
    """FastAPI dependency yielding an async database session."""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db(eng=None):
    """Create all tables, with retry logic for postgres startup.

    Retries up to 5 times with a 2-second sleep between attempts.
    """
    target_engine = eng or engine
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            async with target_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")
            return
        except Exception as exc:
            logger.warning(
                "init_db attempt %d/%d failed: %s", attempt, max_retries, exc
            )
            if attempt < max_retries:
                await asyncio.sleep(2)
            else:
                raise
