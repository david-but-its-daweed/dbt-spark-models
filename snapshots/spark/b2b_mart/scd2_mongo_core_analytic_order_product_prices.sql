{% snapshot scd2_mongo_core_analytic_order_product_prices %}

{{
    config(
    meta = {
      'model_owner' : '@tigran'
    },
    target_schema='b2b_mart',
    unique_key='procurement_order_id',

    strategy='timestamp',
    updated_at='updated_ts_msk',
    file_format='delta'
    )
}}

SELECT
    _id AS procurement_order_id,

    final_prices,
    init_prices,

    MILLIS_TO_TS_MSK(ctms) AS created_ts_msk,
    MILLIS_TO_TS_MSK(utms) AS updated_ts_msk
FROM {{ source('mongo', 'b2b_core_analytic_order_product_prices_daily_snapshot') }}

{% endsnapshot %}