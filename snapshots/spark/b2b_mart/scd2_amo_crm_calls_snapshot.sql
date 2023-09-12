{% snapshot scd2_amo_crm_calls_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='callId',

      file_format='delta',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_amo_crm_calls_daily_snapshot') }}
{% endsnapshot %}
