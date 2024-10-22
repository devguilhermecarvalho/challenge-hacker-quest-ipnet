#!/bin/bash
set -e

# Run the main ETL function
python main.py

# Run dbt commands for transformation
cd dbt_validations
dbt deps
dbt seed
dbt run
