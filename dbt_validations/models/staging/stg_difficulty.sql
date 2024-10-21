{{ config(materialized='view') }}

SELECT 
  CAST(column1 AS INT) AS level_id,
  CAST(column2 AS INT) AS difficulty_score
FROM {{ source('etl_hackerquest', 'difficulty') }}
