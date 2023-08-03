{% snapshot scd2_mongo_product_state %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='product_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

SELECT  _id as product_id,
        r as reject_reason,
        s as status,
        millis_to_ts_msk(utms) as update_ts_msk
FROM {{ source('mongo', 'b2b_core_product_states_daily_snapshot') }}
{% endsnapshot %}
