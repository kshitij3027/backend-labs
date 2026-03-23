"""Registry for map and reduce functions."""

MAP_FUNCTIONS: dict[str, callable] = {}
REDUCE_FUNCTIONS: dict[str, callable] = {}


def register_map(name: str):
    """Decorator to register a map function by name."""

    def decorator(fn):
        MAP_FUNCTIONS[name] = fn
        return fn

    return decorator


def register_reduce(name: str):
    """Decorator to register a reduce function by name."""

    def decorator(fn):
        REDUCE_FUNCTIONS[name] = fn
        return fn

    return decorator


def get_map_fn(name: str):
    """Retrieve a registered map function by name."""
    if name not in MAP_FUNCTIONS:
        raise KeyError(f"Map function '{name}' not registered. Available: {list(MAP_FUNCTIONS.keys())}")
    return MAP_FUNCTIONS[name]


def get_reduce_fn(name: str):
    """Retrieve a registered reduce function by name."""
    if name not in REDUCE_FUNCTIONS:
        raise KeyError(f"Reduce function '{name}' not registered. Available: {list(REDUCE_FUNCTIONS.keys())}")
    return REDUCE_FUNCTIONS[name]
