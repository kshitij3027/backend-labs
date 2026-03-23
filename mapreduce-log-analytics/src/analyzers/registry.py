"""Analyzer function registry with decorator-based registration."""

from typing import Any, Callable

MAP_FUNCTIONS: dict[str, Callable] = {}
REDUCE_FUNCTIONS: dict[str, Callable] = {}
POSTPROCESS_FUNCTIONS: dict[str, Callable] = {}


def register_map(name: str):
    """Decorator to register a map function."""
    def decorator(fn: Callable):
        MAP_FUNCTIONS[name] = fn
        return fn
    return decorator


def register_reduce(name: str):
    """Decorator to register a reduce function."""
    def decorator(fn: Callable):
        REDUCE_FUNCTIONS[name] = fn
        return fn
    return decorator


def register_postprocess(name: str):
    """Decorator to register a postprocess function."""
    def decorator(fn: Callable):
        POSTPROCESS_FUNCTIONS[name] = fn
        return fn
    return decorator


def get_map_fn(name: str) -> Callable:
    if name not in MAP_FUNCTIONS:
        raise KeyError(f"Unknown map function: {name}")
    return MAP_FUNCTIONS[name]


def get_reduce_fn(name: str) -> Callable:
    if name not in REDUCE_FUNCTIONS:
        raise KeyError(f"Unknown reduce function: {name}")
    return REDUCE_FUNCTIONS[name]


def get_postprocess_fn(name: str) -> Callable | None:
    return POSTPROCESS_FUNCTIONS.get(name)


def list_analyzers() -> dict[str, dict]:
    """List all registered analyzers with their available functions."""
    all_names = set(MAP_FUNCTIONS.keys()) | set(REDUCE_FUNCTIONS.keys())
    result = {}
    for name in sorted(all_names):
        result[name] = {
            "has_map": name in MAP_FUNCTIONS,
            "has_reduce": name in REDUCE_FUNCTIONS,
            "has_postprocess": name in POSTPROCESS_FUNCTIONS,
        }
    return result
