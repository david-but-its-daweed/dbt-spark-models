{% snapshot scd2_product_states_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


    SELECT
        *,
        MILLIS_TO_TS_MSK(utms) AS update_ts_msk
    FROM {{ source('mongo', 'b2b_product_product_states_daily_snapshot') }}
{% endsnapshot %}
