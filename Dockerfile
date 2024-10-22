FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y git

RUN pip install --no-cache-dir dbt-bigquery

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x entrypoint.sh

ENV DBT_PROFILES_DIR=/app/dbt_validations

ENTRYPOINT ["./entrypoint.sh"]
