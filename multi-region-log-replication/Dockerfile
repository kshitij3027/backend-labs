FROM python:3.11-slim

# Run the app under a non-root user.
RUN useradd --create-home --shell /usr/sbin/nologin app

WORKDIR /app

# Install Python deps first so they cache across source-file changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source. ``web/`` is included so the static dashboard
# (landing in commit 4) is served from inside the container; ``scripts/`` is
# copied in so ``make demo`` (``python -m scripts.demo``) can run inside
# the container without bind-mounts.
COPY src/ ./src/
COPY web/ ./web/
COPY scripts/ ./scripts/

# Ownership + non-root drop.
RUN chown -R app:app /app
USER app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

CMD ["python", "-m", "src"]
