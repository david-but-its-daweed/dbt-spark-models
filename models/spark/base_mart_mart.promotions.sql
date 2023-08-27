WITH source AS (
    SELECT * FROM {{ source('mart', 'promotions') }}
),

renamed AS (
    SELECT
        {{ adapter.quote("promo_id") }},
        {{ adapter.quote("promo_title") }},
        {{ adapter.quote("promo_start_time_utc") }},
        {{ adapter.quote("promo_end_time_utc") }},
        {{ adapter.quote("product_group_id") }},
        {{ adapter.quote("product_id") }}
    FROM source
)

SELECT * FROM renamed