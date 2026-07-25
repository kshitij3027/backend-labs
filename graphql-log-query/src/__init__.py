"""GraphQL Log Query Platform — application package.

Both images set ``PYTHONPATH=/app`` and ``COPY src/ ./src/``, so every import in this project is
absolute from this package root (``from src.config import get_settings``). Keeping it a real
package rather than a namespace one means ``python -m uvicorn src.main:app`` resolves identically
inside the container, inside the tester image, and on a host running pytest from the project root.
"""
