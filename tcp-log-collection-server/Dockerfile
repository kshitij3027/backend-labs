FROM python:3.12-alpine

WORKDIR /app

COPY src/ src/
COPY main.py test_client.py* ./

EXPOSE 9000

CMD ["python", "main.py"]
