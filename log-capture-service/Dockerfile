FROM python:3.12-alpine
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY config.example.yml ./config.yml
RUN mkdir -p /app/logs /app/collected_logs
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "src.main", "--log-files", "/app/logs/app.log", \
     "--output-dir", "/app/collected_logs", "--config", "/app/config.yml"]
