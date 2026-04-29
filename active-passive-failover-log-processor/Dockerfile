FROM python:3.11-slim AS runtime
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY scripts/ ./scripts/
ENV PYTHONUNBUFFERED=1
EXPOSE 8001
CMD ["python", "-m", "src"]
