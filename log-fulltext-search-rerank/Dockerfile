## Multi-stage build for the intelligent log search & ranking engine.
##
## Stage 1 (builder) installs resolved wheels into /install so the
## final image never ships pip caches or a compiler toolchain. Stage 2
## rehydrates those wheels on top of a clean python:3.12-slim, bakes
## the NLTK corpora the tokenizer will depend on (stopwords, wordnet,
## punkt, punkt_tab), and drops privileges to a non-root ``app`` user.
##
## NLTK data is baked at build time — the runtime path never calls
## ``nltk.download()``. This keeps startup deterministic and works
## offline.

# ------------------------------ builder ------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ------------------------------ runtime ------------------------------
FROM python:3.12-slim AS runtime

# curl is used by the compose healthcheck and by start.sh. slim does
# not ship it by default.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root execution — uid 1000 matches the reference images in this
# repo so bind-mounts line up.
RUN useradd --create-home --uid 1000 app

WORKDIR /app

# Carry over installed site-packages + console scripts from builder.
COPY --from=builder /install /usr/local

# Bake NLTK corpora into a shared location before the USER switch so
# they are owned by root and world-readable. Commit 01 does not use
# NLTK yet, but later commits (tokenizer, query parser) depend on
# these corpora — baking now avoids a rebuild when we wire the
# tokenizer in.
ENV NLTK_DATA=/usr/share/nltk_data
RUN python -m nltk.downloader -d /usr/share/nltk_data \
        stopwords wordnet punkt punkt_tab \
    && chmod -R 755 /usr/share/nltk_data

# App source.
COPY src/ /app/src/
COPY templates/ /app/templates/
COPY static/ /app/static/

RUN chown -R app:app /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

USER app

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
