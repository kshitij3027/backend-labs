FROM python:3.11-slim

# Run the app under a non-root user.
RUN useradd --create-home --shell /usr/sbin/nologin app

WORKDIR /app

# Install Python deps first so they cache across source-file changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source. ``web/`` is included so the static dashboard
# (landing in commit 4) is served from inside the container; ``web/.gitkeep``
# keeps the directory present until the real index.html arrives.
COPY src/ ./src/
COPY web/ ./web/

# Ownership + non-root drop.
RUN chown -R app:app /app
USER app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

CMD ["python", "-m", "src"]
