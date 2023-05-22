{% snapshot scd2_product_states_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT *, millis_to_ts_msk(utms)  AS update_ts_msk
FROM {{ source('mongo', 'b2b_core_product_states_daily_snapshot') }}
{% endsnapshot %}
