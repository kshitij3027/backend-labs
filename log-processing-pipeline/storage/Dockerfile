FROM python:3.12-alpine
RUN pip install --no-cache-dir pyyaml==6.0.2
WORKDIR /app
COPY shared/ shared/
COPY storage/ storage/
COPY config.yml config.yml
CMD ["python", "-m", "storage.main"]
