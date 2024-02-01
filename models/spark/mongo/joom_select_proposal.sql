{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov',
      'team': 'merchant',
    },
) }}
SELECT
    _id AS proposal_id,
    ct AS created_time,
    ut AS updated_time,
    pid AS product_id,
    mid AS merchant_id,
    TRANSFORM(sh, col -> NAMED_STRUCT(
        'status', CASE
            WHEN col.s = 1 THEN 'pending'
            WHEN col.s = 2 THEN 'approved'
            WHEN col.s = 3 THEN 'cancelled'
            ELSE ''
        END,
        'updated_time', col.ut,
        'merchant_variant_prices', TRANSFORM(col.mps, price -> NAMED_STRUCT(
            'variant_id', price.vid,
            'price', CAST(price.p AS DOUBLE)
        ))
    )) AS status_history,
    TRANSFORM(tps, price -> NAMED_STRUCT(
        'variant_id', price.vid,
        'price', CAST(price.p AS DOUBLE)
    )) AS target_variant_prices,
    IF(ci IS NOT NULL, NAMED_STRUCT(
        'reason', CASE
            WHEN ci.r = 1 THEN 'merchantRemovedFromJoomSelect'
            WHEN ci.r = 2 THEN 'productRemovedFromJoomSelect'
            WHEN ci.r = 3 THEN 'tooLongFulfillment'
            WHEN ci.r = 4 THEN 'replacedByOtherProduct'
            WHEN ci.r = 5 THEN 'replacedByOtherProposal'
            WHEN ci.r = 6 THEN 'tooLowPrices'
            WHEN ci.r = 7 THEN 'other'
            ELSE ''
        END,
        'source', CASE
            WHEN ci.s = 1 THEN 'merchant'
            WHEN ci.s = 2 THEN 'joom'
            ELSE ''
        END
    ), NULL) AS cancel_info
FROM {{ source('mongo', 'product_merchant_joom_select_proposals_daily_snapshot') }}
