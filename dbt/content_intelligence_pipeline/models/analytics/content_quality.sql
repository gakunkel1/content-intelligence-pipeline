{{
    config(
        materialized='table',
        tags=['analytics'],
        schema='analytics'
    )
}}

SELECT  p.id as product_id,
		p.title,
		e.brand_consistency_score,
		e.brand_score_reasoning,
		e.qa_flags
FROM {{ ref('product_snapshot') }} p
LEFT JOIN {{ ref('stg_enrichments') }} e ON e.product_id = p.id
WHERE p.dbt_valid_to IS NULL
ORDER BY p.id