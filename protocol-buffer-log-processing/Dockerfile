FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ proto/
COPY compile_proto.sh .
RUN chmod +x compile_proto.sh && ./compile_proto.sh

COPY src/ src/
COPY . .

CMD ["python", "main.py"]
