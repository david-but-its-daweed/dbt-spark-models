{% snapshot scd2_form_with_status %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='form_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}



select 
_id as form_id,
context,
ctxId as merchant_order_id, 
scndCtxId as product_id, 
payloadNew as payload,
type,
statusId as status_id,
millis_to_ts_msk(ctms)  AS created_ts_msk ,
millis_to_ts_msk(utms)  AS update_ts_msk
from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}

{% endsnapshot %}
