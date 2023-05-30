{% snapshot scd2_order_products_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='_id',

      file_format='delta',
      strategy='check',
      check_cols=['ctms'],
      invalidate_hard_deletes=True,
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
{% endsnapshot %}
