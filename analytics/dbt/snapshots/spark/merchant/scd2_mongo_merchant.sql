{% snapshot scd2_mongo_merchant %}

{{
    config(
      target_schema='merchant',
      unique_key='merchant_id',
      strategy='timestamp',
      updated_at='updated_time',
      file_format='delta',
      invalidate_hard_deletes=True,
      meta = {
          'bigquery_load': 'true',
      }
    )
}}


select merchant_id,
       created_time,
       updated_time,
       activation_time,
       name,
       origin,
       enabled,
       disablingReason,
       disablingNote
from {{ref('merchant')}}

{% endsnapshot %}
