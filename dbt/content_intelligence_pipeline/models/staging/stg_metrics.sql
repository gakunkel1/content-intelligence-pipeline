{{
    config(
        materialized='view',
        tags=['stg_analytics'],
        schema='staging'
    )
}}

SELECT  id,
        product_id,
        model,
        input_tokens,
        output_tokens,
        COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0) as total_tokens,
        latency_ms,
        latency_ms * 1.0 / 1000 as latency_sec,
        CASE 
            WHEN latency_ms IS NULL OR latency_ms = 0 THEN 0
            ELSE (COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0))
                        / (latency_ms * 1.0 / 1000)
            END as total_tokens_per_sec,
        enriched_at,
        is_success,
        error_message
FROM {{ source('enriched', 'llm_metrics') }}