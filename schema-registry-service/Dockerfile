FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ schemas/
COPY src/ src/
COPY templates/ templates/
COPY data/ data/

RUN useradd -m appuser && chown -R appuser:appuser /app/data
USER appuser

EXPOSE 8080

CMD ["python", "-m", "src.app"]
