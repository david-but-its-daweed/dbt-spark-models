{% snapshot scd2_mongo_product_jpa_matches_daily_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@kirill_melnikov'
    },
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT *, millis_to_ts_msk(uber_loader_eventTimeMs)  AS update_ts_msk
FROM {{ source('mongo', 'b2b_core_product_jpa_matches_daily_snapshot') }}
{% endsnapshot %}
