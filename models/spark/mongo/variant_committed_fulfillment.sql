{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@slava',
      'team': 'merchant',
    },
) }}
WITH variant_cfts AS (
    SELECT
        _id AS product_id,
        EXPLODE(vcft) AS variant_cft,
    FROM {{ source('mongo', 'product_committed_fulfillment_daily_snapshot') }}
)

SELECT
    product_id,
    variant_cft.vid AS variant_id,
    variant_cft.cft AS cft
FROM variant_cfts
