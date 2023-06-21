{% snapshot scd2_offer_products_snapshot %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='offer_product_id',

      strategy='check',
      check_cols=['created_time_msk', 'product_id', 'offer_id', 'disabled', 'type'],
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


select 
_id as offer_product_id, 
id as product_id,
offerId as offer_id,
trademark,
hsCode as hs_code,
manufacturerId as manufacturer_id,
name, 
nameInv as name_inv,
type,
disabled,
link,
millis_to_ts_msk(ctms) as created_time_msk
from {{ source('mongo', 'b2b_core_offer_products_daily_snapshot') }}
{% endsnapshot %}
