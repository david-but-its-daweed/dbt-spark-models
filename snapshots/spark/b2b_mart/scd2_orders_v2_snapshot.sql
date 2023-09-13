{% snapshot scd2_orders_v2_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}

SELECT
    _id,
    brokerId,
    comissionRate,
    ctms,
    currencies,
    currencyRates,
    dealID,
    deliveryScheme,
    deliveryTimeDays,
    descr,
    friendlyId,
    interactionId,
    linehaulChannelID,
    otherPrices,
    struct(
        payment.advancePercent AS advancePercent,
        payment.clientCurrency AS clientCurrency,
        payment.completePaymentAfter AS completePaymentAfter,
        payment.paymentChannel AS paymentChannel,
        payment.paymentType AS paymentType,
        payment.paymentWithinDaysAdvance AS paymentWithinDaysAdvance,
        payment.paymentWithinDaysComplete AS paymentWithinDaysComplete
    ) AS payment,
    popupReqId,
    prices,
    roleSet,
    state,
    tags,
    utms,
    csmr,
    millis_to_ts_msk(utms) AS update_ts_msk
FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }}
{% endsnapshot %}
