FROM python:3.12-alpine

WORKDIR /app

COPY src/ ./src/

RUN mkdir -p /app/logs

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.main"]
