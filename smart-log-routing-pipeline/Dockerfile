FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
COPY scripts/ scripts/
COPY config/ config/
COPY web/ web/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
CMD ["python", "scripts/wait_for_rabbitmq.py"]
