{% snapshot scd2_customer_plans_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='uid',

      strategy='timestamp',
      updated_at='utms',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_core_customer_plans_daily_snapshot') }}
{% endsnapshot %}
