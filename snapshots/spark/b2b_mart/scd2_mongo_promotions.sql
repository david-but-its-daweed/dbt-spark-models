{% snapshot scd2_mongo_promotions %}

{{
    config(
    meta = {
      'model_owner' : '@abadoyan'
    },
      target_schema='b2b_mart',
      unique_key='promotion_id',

      strategy='timestamp',
      updated_at='updated_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT _id AS promotion_id,
       name AS promotion_name,
       alias,
       locs AS position,
       maxcount AS max_count,
       pgs,
       MILLIS_TO_TS_MSK(ctms) AS created_ts_msk,
       MILLIS_TO_TS_MSK(utms) AS updated_ts_msk
FROM {{ source('mongo', 'b2b_core_promotions_daily_snapshot') }}

{% endsnapshot %}
