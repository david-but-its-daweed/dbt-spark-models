{% snapshot scd2_sdr_plans_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='unique_key',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT *, millis_to_ts_msk(utms)  AS update_ts_msk, mid||hn||tp as unique_key
FROM {{ source('mongo', 'b2b_core_admin_user_sdr_plans_daily_snapshot') }}
{% endsnapshot %}
