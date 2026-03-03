FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/
COPY compile_proto.sh .

# Compile proto files
RUN chmod +x compile_proto.sh && ./compile_proto.sh

# Default environment
ENV NODE_ID=node-1
ENV HOST=0.0.0.0
ENV PORT=5001
ENV PEERS=""

CMD ["python", "-m", "src"]
