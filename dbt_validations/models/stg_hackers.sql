-- dbt/models/staging/stg_hackers.sql
select * from {{ source('etl_hackerquest', 'hackers') }}