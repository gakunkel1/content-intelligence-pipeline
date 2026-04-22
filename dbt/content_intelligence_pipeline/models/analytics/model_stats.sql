{{
    config(
        materialized='table',
        tags=['analytics'],
        schema='analytics'
    )
}}

SELECT model,
       AVG(latency_sec) as avg_latency_sec,
       AVG(total_tokens_per_sec) as avg_total_tokens_per_sec,
       AVG(is_success::INT) as success_rate,
       COUNT(CASE WHEN is_success = TRUE THEN 1 ELSE NULL END) as success_count,
       COUNT(CASE WHEN is_success = FALSE THEN 1 ELSE NULL END) as failure_count
FROM {{ ref('stg_metrics') }}
GROUP BY model