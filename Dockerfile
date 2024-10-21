# Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir dbt-bigquery

COPY . .

ENV DBT_PROFILES_DIR=/app/dbt_validations

EXPOSE 8080

CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]