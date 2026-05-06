FROM python:3.11-slim

# curl is needed for the docker-compose healthcheck against /health.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Run the app under a non-root user.
RUN useradd --create-home --shell /usr/sbin/nologin app

WORKDIR /app

# Install Python deps first so they cache across source-file changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-create runtime directories so later commits (web dashboard, scripts,
# templates) can drop files in without breaking the image. These dirs are
# expected to be COPY'd in once they exist on disk.
RUN mkdir -p ./web/static ./templates ./scripts

# Copy the application source. ``web/``, ``templates/``, ``scripts/`` will be
# added in later commits — for Commit 1 only ``src/`` is real.
COPY src/ ./src/

# Ownership + non-root drop.
RUN chown -R app:app /app
USER app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000

EXPOSE 8000

CMD ["python", "-m", "src"]
