{% snapshot scd2_mongo_quotes %}

{{
    config(
    meta = {
      'model_owner' : '@abadoyan'
    },
      target_schema='b2b_mart',
      unique_key='_id',
      strategy='timestamp',
      updated_at='created_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}


SELECT _id,
       cartPrices,
       cnpj,
       comment,
       commercialProposalDocSentDate,
       MILLIS_TO_TS_MSK(createdTimeMs) AS created_ts_msk,
       currencyRates,
       dealFriendlyId,
       dealId,
       deliveryChannel,
       index,
       paymentMethod,
       products,
       stage,
       subsidies
FROM {{ source('mongo', 'b2b_core_quotes_daily_snapshot') }}

{% endsnapshot %}
