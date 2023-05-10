{% snapshot scd2_popup_requests_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='callId',

      file_format='delta',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_popup_requests_daily_snapshot') }}
{% endsnapshot %}
