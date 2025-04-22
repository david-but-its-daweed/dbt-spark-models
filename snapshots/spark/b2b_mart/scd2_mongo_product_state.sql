{% snapshot scd2_mongo_product_state %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
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
        r AS reject_reason,
        s AS status,
        MILLIS_TO_TS_MSK(utms) AS update_ts_msk
    FROM {{ source('mongo', 'b2b_product_product_states_daily_snapshot') }}
{% endsnapshot %}
