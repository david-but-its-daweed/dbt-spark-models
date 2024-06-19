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

    SELECT
        _id AS calculation_id,
        brokerid AS broker_id,
        channelid AS delivery_channel_id,
        commission AS commission,
        MILLIS_TO_TS_MSK(ctms+1) AS created_ts_msk,
        dealid AS deal_id,
        deliveryscheme AS delivery_scheme,
        isdocumentoutdated AS is_document_outdated,
        name,
        offerids AS offer_ids,
        offeroverrides AS offer_overrides,
        paymentccy AS payment_ccy,
        priceoverridesv2 AS price_overrides,
        rates,
        samplepriceoverridesv2 AS sample_price_overrides,
        subsidy,
        MILLIS_TO_TS_MSK(ctms) AS update_ts_msk,
        variantoverrides AS variant_overrides,
        paymentmethod AS payment_method
    FROM {{ source('mongo', 'b2b_core_calculations_daily_snapshot') }}


{% endsnapshot %}
