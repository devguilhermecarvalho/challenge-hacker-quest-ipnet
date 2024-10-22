FROM python:3.9-slim

# Install necessary packages
RUN apt-get update && apt-get install -y git

# Install dbt-bigquery
RUN pip install --no-cache-dir dbt-bigquery

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure entrypoint is executable
RUN chmod +x entrypoint.sh

# Set environment variable for dbt profiles
ENV DBT_PROFILES_DIR=/app/dbt_validations

ENTRYPOINT ["./entrypoint.sh"]