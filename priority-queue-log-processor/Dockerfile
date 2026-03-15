FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
COPY templates/ templates/
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD curl -f http://localhost:8080/health || exit 1
CMD ["python", "-m", "src.main"]
