{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

SELECT _id AS quote_id,
       cartPrices AS cart_prices,
       cnpj,
       comment,
       commercialProposalDocSentDate AS commercial_proposal_doc_sent_date,
       created_ts_msk,
       currencyRates AS currency_rates,
       dealFriendlyId AS deal_friendly_id,
       dealId AS deal_id,
       deliveryChannel AS delivery_channel,
       index,
       paymentMethod AS payment_method,
       products,
       stage,
       subsidies,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_quotes') }}
WHERE dbt_valid_to IS NULL
