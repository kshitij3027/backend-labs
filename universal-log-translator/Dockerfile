FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY proto/ proto/
COPY schemas/ schemas/
COPY sample_logs/ sample_logs/
COPY scripts/ scripts/
COPY compile_proto.sh .
COPY verify.sh .

# Proto compilation happens at build time (if proto/ exists)
RUN if [ -f compile_proto.sh ]; then chmod +x compile_proto.sh && bash compile_proto.sh || true; fi

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app/sample_logs
USER appuser

ENTRYPOINT ["python", "-m", "src"]
