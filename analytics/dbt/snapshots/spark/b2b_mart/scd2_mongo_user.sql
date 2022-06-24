{% snapshot scd2_mongo_user %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='user_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

SELECT _id AS user_id,
    anon,
    millis_to_ts_msk(ctms) AS created_ts_msk,
    millis_to_ts_msk(utms) AS update_ts_msk,
    addr.country AS pref_country,
    valSt.rjRsn AS reject_reason,
    valSt.st AS validation_status,
    0 AS staff
FROM {{ source('mongo', 'b2b_core_users_daily_snapshot') }}
{% endsnapshot %}
