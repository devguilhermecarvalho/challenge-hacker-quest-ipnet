#!/bin/bash
set -e

# Run main.py
python main.py

# Run dbt commands
cd dbt_validations
dbt deps
dbt seed
dbt run
