{% snapshot scd2_mongo_merchant %}

{{
    config(
      target_schema='example',
      unique_key='merchant_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

select
  merchant_id,
  name,
  origin,
  millis_to_ts_msk(created_time) as create_ts_msk,
  millis_to_ts_msk(updated_time) as update_ts_msk
from {{ source('default', 'mongo_merchant') }}

{% endsnapshot %}
