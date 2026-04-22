{{
    config(
        materialized='table',
        tags=['analytics'],
        schema='analytics'
    )
}}

SELECT  p.id as product_id,
		p.title,
		p.category,
		e.item_subcategory as subcategory,
		p.price::MONEY,
		p.image,
		e.seo_description,
		p.rating_score,
		p.rating_count,
		e.item_tags,
		e.target_audience
FROM {{ ref('product_snapshot') }} p
LEFT JOIN {{ ref('stg_enrichments') }} e ON e.product_id = p.id
WHERE p.dbt_valid_to IS NULL
ORDER BY p.id