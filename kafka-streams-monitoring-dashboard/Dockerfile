FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
COPY templates/ templates/
COPY static/ static/
COPY scripts/ scripts/
EXPOSE 5000
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 CMD curl -f http://localhost:5000/health || exit 1
CMD ["python", "-m", "src.main"]
