FROM python:3.12-alpine

WORKDIR /app

COPY src/ src/
COPY main.py ./

EXPOSE 5514/udp

CMD ["python", "main.py"]
