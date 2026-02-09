FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application + tests
COPY src ./src
COPY tests ./tests

ENV PYTHONPATH=/app
CMD ["python", "-m", "src.consumer"]
