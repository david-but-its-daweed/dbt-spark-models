{% snapshot scd2_mongo_product_prices_daily_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@mkirusha'
    },
      target_schema='b2b_mart',
      unique_key='product_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

    SELECT
        _id AS product_id,
        P.brMin.amount AS min_price,
        P.brMin.ccy AS min_price_ccy,
        MILLIS_TO_TS_MSK(utms) AS update_ts_msk
    FROM {{ source('mongo', 'b2b_product_product_prices_daily_snapshot') }}
{% endsnapshot %}
