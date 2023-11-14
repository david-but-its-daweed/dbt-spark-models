{% snapshot scd2_calculations_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='calculation_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}

select
    _id as calculation_id,
    brokerId as broker_id,
    channelId as delivery_channel_id,
    commission as commission,
    millis_to_ts_msk(ctms) as created_ts_msk,
    dealId as deal_id,
    deliveryScheme as delivery_scheme,
    isDocumentOutdated as is_document_outdated,
    name,
    offerIds as offer_ids,
    offerOverrides as offer_overrides,
    paymentCcy as payment_ccy,
    priceOverridesV2 as price_overrides,
    rates,
    samplePriceOverridesV2 as sample_price_overrides,
    subsidy,
    millis_to_ts_msk(ctms) as update_ts_msk,
    variantOverrides as variant_overrides
from {{ source('mongo', 'b2b_core_calculations_daily_snapshot') }}


{% endsnapshot %}
