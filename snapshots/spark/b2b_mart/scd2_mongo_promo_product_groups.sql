{% snapshot scd2_mongo_promo_product_groups %}

{{
    config(
    meta = {
      'model_owner' : '@abadoyan'
    },
      target_schema='b2b_mart',
      unique_key='product_groups_id',
      strategy='timestamp',
      updated_at='updated_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


    SELECT
        _id AS product_groups_id,
        name AS product_groups_name,
        content,
        MILLIS_TO_TS_MSK(ctms) AS created_ts_msk,
        MILLIS_TO_TS_MSK(utms) AS updated_ts_msk
    FROM {{ source('mongo', 'b2b_core_promo_product_groups_daily_snapshot') }}

{% endsnapshot %}
