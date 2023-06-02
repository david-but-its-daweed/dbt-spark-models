{% snapshot scd2_order_products_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='create_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT *, millis_to_ts_msk(ctms)  AS create_ts_msk
FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
{% endsnapshot %}
