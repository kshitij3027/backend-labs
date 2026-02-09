FROM python:3.12-alpine
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
RUN mkdir -p /app/logs /app/collected_logs
ENV PYTHONUNBUFFERED=1
CMD ["python", "-c", "from src.config import Config, load_yaml_config, load_config; print('imports OK')"]
