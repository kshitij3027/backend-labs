FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY demo.py .
COPY samples/ ./samples/

RUN mkdir -p /app/logs /app/parsed_logs

ENV PYTHONUNBUFFERED=1
ENV LOG_INPUT_DIR=/app/logs
ENV LOG_OUTPUT_DIR=/app/parsed_logs

CMD ["python", "-m", "src.service"]
