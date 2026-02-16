FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY templates/ templates/
COPY main.py ./

EXPOSE 5514/udp
EXPOSE 8080

CMD ["python", "main.py"]
