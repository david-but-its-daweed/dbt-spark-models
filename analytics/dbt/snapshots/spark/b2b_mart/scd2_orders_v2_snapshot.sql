{% snapshot scd2_orders_v2_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='utms',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }}
{% endsnapshot %}
