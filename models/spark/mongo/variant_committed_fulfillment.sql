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
        EXPLODE(vcft) as variant_cft,
    FROM {{ source('mongo', 'product_committed_fulfillment_daily_snapshot') }}
),

SELECT
    product_id,
    variant_cft.vid as variant_id,
    variant_cft.cft as cft
FROM variant_cfts
