{% snapshot scd2_amo_crm_calls_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='callId',

      file_format='delta',
      strategy='check',
      check_cols='all',
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_amo_crm_calls_daily_snapshot') }}
{% endsnapshot %}
