{% snapshot product_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        tags=['product_snapshot'],
        check_cols=['title', 'description', 'price', 'category', 'image', 'rating_score', 'rating_count']
    )
}}

SELECT id,
       title,
       description,
       price,
       category,
       image,
       rating_score,
       rating_count,
       ingested_at
FROM {{ ref('stg_products') }}

{% endsnapshot %}