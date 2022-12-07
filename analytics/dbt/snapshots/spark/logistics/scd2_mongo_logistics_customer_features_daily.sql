{% snapshot scd2_mongo_logistics_customer_features_daily_snapshot %}

{{
    config(
      target_schema='logistics',
      unique_key='_id',

      strategy='timestamp',
      updated_at='updatedTime',
      file_format='delta'
    )
}}

select
  _id,
  features,
  updatedTime
FROM {{ source('mongo', 'logistics_customer_features_daily_snapshot') }}

{% endsnapshot %}
