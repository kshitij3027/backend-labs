"""Base adapter class for log format parsing."""
from abc import ABC, abstractmethod
from src.models import ParsedLog


class LogFormatAdapter(ABC):
    """Abstract base class for log format adapters."""

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Return the name of the log format this adapter handles."""
        pass

    @abstractmethod
    def can_handle(self, line: str) -> float:
        """
        Determine if this adapter can handle the given log line.

        Returns a confidence score between 0.0 and 1.0.
        0.0 = definitely cannot handle
        1.0 = definitely can handle
        """
        pass

    @abstractmethod
    def parse(self, line: str) -> ParsedLog:
        """
        Parse a log line into a ParsedLog instance.

        Should set source_format to self.format_name.
        """
        pass
