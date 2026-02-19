FROM python:3.12-alpine

RUN apk add --no-cache openssl

WORKDIR /app

COPY generate_certs.sh ./
RUN chmod +x generate_certs.sh && ./generate_certs.sh /app/certs

COPY src/ src/
COPY server_main.py healthcheck.py ./

ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8443
ENV CERT_FILE=/app/certs/server.crt
ENV KEY_FILE=/app/certs/server.key
ENV LOG_DIR=/app/logs

EXPOSE 8443

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python", "healthcheck.py"]

CMD ["python", "-u", "server_main.py"]
