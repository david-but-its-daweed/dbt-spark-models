{% snapshot scd2_mongo_customer_markets %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

SELECT concat_ws('-', customer_id, COALESCE(market_id,0), utms) as id,
    customer_id,
    update_ts_msk,
    market_id
FROM (
    SELECT
       _id                          AS customer_id,
       utms,
       millis_to_ts_msk(utms)       AS update_ts_msk,
       explode(markets)             AS market_id
    FROM {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
 )
    {% endsnapshot %}
