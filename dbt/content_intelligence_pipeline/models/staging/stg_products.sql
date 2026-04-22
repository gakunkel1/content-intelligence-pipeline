{{
    config(
        materialized='view',
        tags=['stg_products'],
        schema='staging'
    )
}}

-- Deduplicate records based on id and keep the latest ingested_at
with src as (
    SELECT
        id,
        title,
        description,
        price,
        category,
        image,
        NULLIF(rating->>'rate', '')::float as rating_score,
        NULLIF(rating->>'count', '')::int as rating_count,
        ingested_at,
        row_number() over (partition by id order by ingested_at desc) as rn
    FROM {{ source('raw', 'products') }}
)

-- Output
SELECT id,
       title,
       description,
       price,
       category,
       image,
       rating_score,
       rating_count,
       ingested_at
FROM src
WHERE rn = 1