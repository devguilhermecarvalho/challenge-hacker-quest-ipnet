# Dockerfile

# Use Python as the base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the container and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt and the BigQuery adapter for dbt
RUN pip install --no-cache-dir dbt-bigquery

# Copy the entire project into the container
COPY . .

# Set the DBT profiles directory environment variable
ENV DBT_PROFILES_DIR=/app/dbt_validations

# Expose the port to communicate with Cloud Run
EXPOSE 8080

# Start the Flask app with Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]
