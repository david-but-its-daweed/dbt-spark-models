{% snapshot scd2_customer_plans_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='uid',

      strategy='timestamp',
      updated_at='utms',
      file_format='delta'
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_customer_plans_daily_snapshot') }}
{% endsnapshot %}
