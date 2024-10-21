{{ config(materialized='view') }}

SELECT 
  SAFE_CAST(column1 AS INT64) AS challenge_id,
  SAFE_CAST(column2 AS INT64) AS hacker_id,
  SAFE_CAST(column3 AS INT64) AS difficulty_level
FROM {{ source('etl_hackerquest', 'challenges') }}
WHERE 
  SAFE_CAST(column1 AS INT64) IS NOT NULL
  AND SAFE_CAST(column2 AS INT64) IS NOT NULL
  AND SAFE_CAST(column3 AS INT64) IS NOT NULL