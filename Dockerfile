# Dockerfile – Chicago Taxi Lakehouse Pipeline
# docker build -t chicago-taxi-pipeline .

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/      ./src/
COPY sql/      ./sql/
COPY main.py   .

RUN mkdir -p data/raw data/processed data/validated data/reports logs

ENV PG_HOST=postgres
ENV PG_PORT=5432
ENV PG_DB=chicago_taxi
ENV PG_USER=taxi_user
ENV PG_PASSWORD=taxi_password

CMD ["python", "main.py"]
