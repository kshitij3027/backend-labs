"""Base handler ABC with __init_subclass__ auto-registration."""
from abc import ABC, abstractmethod
from typing import ClassVar

from src.models import LogEntry


class BaseHandler(ABC):
    """Abstract base class for log format handlers.

    Subclasses auto-register by declaring format_name keyword:
        class JsonHandler(BaseHandler, format_name="json"):
            ...
    """
    _registry: ClassVar[dict[str, type["BaseHandler"]]] = {}
    format_name: str = ""

    def __init_subclass__(cls, format_name: str = "", **kwargs):
        super().__init_subclass__(**kwargs)
        if format_name:
            cls.format_name = format_name
            BaseHandler._registry[format_name] = cls

    @classmethod
    def get_registry(cls) -> dict[str, type["BaseHandler"]]:
        """Return a copy of the handler registry."""
        return dict(cls._registry)

    @classmethod
    def get_handler(cls, format_name: str) -> "BaseHandler":
        """Get a handler instance by format name."""
        handler_cls = cls._registry.get(format_name)
        if handler_cls is None:
            raise KeyError(f"No handler registered for format: {format_name}")
        return handler_cls()

    @abstractmethod
    def can_handle(self, raw_data: bytes) -> bool:
        """Return True if this handler can parse the given raw data."""
        ...

    @abstractmethod
    def parse(self, raw_data: bytes) -> LogEntry:
        """Parse raw bytes into a LogEntry. Raises ValueError if parsing fails."""
        ...
