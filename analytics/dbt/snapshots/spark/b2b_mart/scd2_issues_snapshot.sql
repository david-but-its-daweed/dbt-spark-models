{% snapshot scd2_issues_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='utms',
      file_format='delta'
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
{% endsnapshot %}
