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
      'model_owner' : '@aleksandrov',
          'team' : 'merchant',
      }
    )
}}


select merchant_id,
       millis_to_ts(created_time)    as created_time,
       millis_to_ts(updated_time)    as updated_time,
       millis_to_ts(activation_time) as activation_time,
       name,
       origin,
       enabled,
       disablingReason,
       disablingNote
from {{ ref('merchant') }}

{% endsnapshot %}
