FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

RUN adduser --disabled-password --no-create-home appuser
USER appuser

EXPOSE 8001 5000

# Default CMD is overridden per service in docker-compose
CMD ["python", "-m", "src.storage.server"]
