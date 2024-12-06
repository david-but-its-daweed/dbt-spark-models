{% snapshot scd2_deals_snapshot %}

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
        attachedSpreadsheets,
        contractId,
        country,
        ctms,
        currencyRates,
        delivery,
        description,
        docsFolderId,
        estimatedEndDate,
        estimatedGmv,
        finalCalculationId,
        finalGmv,
        interactionId,
        legalScheme,
        name,
        orderId,
        STRUCT(
            payment.advancePercent AS advancePercent,
            payment.clientCurrency AS clientCurrency,
            payment.completePaymentAfter AS completePaymentAfter,
            payment.paymentChannel AS paymentChannel,
            payment.paymentType AS paymentType,
            payment.paymentWithinDaysAdvance AS paymentWithinDaysAdvance,
            payment.paymentWithinDaysComplete AS paymentWithinDaysComplete,
            payment.workScheme AS workScheme
        ) AS payment,
        requestType,
        sourcingDealType,
        userId,
        utms,
        workScheme,
        isSelfService,
        MILLIS_TO_TS_MSK(utms + 2) AS update_ts_msk
    FROM {{ source('mongo', 'b2b_core_deals_daily_snapshot') }}

{% endsnapshot %}
