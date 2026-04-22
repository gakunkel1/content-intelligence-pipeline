{{
    config(
        materialized='view',
        tags=['products', 'product_enrichments', 'staging'],
        schema='staging'
    )
}}

SELECT  product_id,
        seo_description,
        brand_consistency_score,
        brand_score_reasoning,
        item_subcategory,
        item_tags,
        target_audience,
        qa_flags,
        enriched_at
FROM {{ source('enriched', 'product_enrichments') }}