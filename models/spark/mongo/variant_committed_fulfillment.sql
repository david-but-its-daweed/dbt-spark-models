{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@slava',
      'team': 'merchant',
    },
) }}
WITH t1 AS (
    SELECT
        _id AS product_id,
        explode(vcft) as variant_cft,
    FROM {{ source('mongo', 'product_committed_fulfillment_daily_snapshot') }}
),

SELECT
    product_id,
    variant_cft.vId as variant_id,
    variant_cft.cft as cft
FROM t1
