"""Pattern matching engine for log alerting.

Loads AlertRule patterns from the database, compiles them once,
and matches incoming log messages against the compiled regexes.
Read-only -- never writes to the database.
"""

import re

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import AlertRule

logger = structlog.get_logger(__name__)


class PatternMatcher:
    """Matches log messages against compiled AlertRule regex patterns."""

    def __init__(self) -> None:
        self._patterns: list[tuple[AlertRule, re.Pattern]] = []

    async def load_patterns(self, db_session: AsyncSession) -> None:
        """Load all enabled alert rules from the database and compile them.

        Each pattern is compiled with ``re.IGNORECASE`` and stored as a
        ``(AlertRule, compiled_regex)`` tuple for fast matching.
        """
        result = await db_session.execute(
            select(AlertRule).where(AlertRule.enabled.is_(True))
        )
        rules = result.scalars().all()

        compiled: list[tuple[AlertRule, re.Pattern]] = []
        for rule in rules:
            try:
                regex = re.compile(rule.pattern, re.IGNORECASE)
                compiled.append((rule, regex))
            except re.error as exc:
                logger.error(
                    "invalid_regex",
                    rule_name=rule.name,
                    pattern=rule.pattern,
                    error=str(exc),
                )

        self._patterns = compiled
        logger.info("patterns_loaded", count=len(self._patterns))

    def match(self, log_message: str) -> list[AlertRule]:
        """Return every AlertRule whose pattern matches *log_message*.

        A single log message can match multiple rules.
        """
        matched: list[AlertRule] = []
        for rule, regex in self._patterns:
            if regex.search(log_message):
                matched.append(rule)
        return matched

    async def reload_patterns(self, db_session: AsyncSession) -> None:
        """Explicit reload alias -- same as :meth:`load_patterns`."""
        await self.load_patterns(db_session)
