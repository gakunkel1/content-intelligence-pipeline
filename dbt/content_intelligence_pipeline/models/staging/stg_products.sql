{{
    config(
        materialized='view',
        tags=['products', 'staging'],
        schema='staging'
    )
}}

SELECT id,
       title,
       description,
       price,
       category,
       image,
       rating,
       ingested_at
FROM {{ source('raw', 'products') }}