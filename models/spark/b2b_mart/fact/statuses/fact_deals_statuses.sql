{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


select 
  deal_id, 
  user_id,
  country,
  owner_email, owner_id, owner_role,
  max(case when next_status is null then status end) as current_status,
  max(case when next_status is null then min_date end) as current_status_date,
  max(case when status = 'RequestRetrieval' then min_date end) as request_retrieval,
  max(case when status = 'RequestRetrieval' then next_status_date end) as request_retrieval_end,
  max(case when status = 'TrialPricing' then min_date end) as trial_pricing,
  max(case when status = 'TrialPricing' then next_status_date end) as trial_pricing_end,
  max(case when status = 'WaitingTrialPricingFeedback' then min_date end) as waiting_trial_pricing_feedback,
  max(case when status = 'WaitingTrialPricingFeedback' then next_status_date end) as waiting_trial_pricing_feedback_end,
  max(case when status = 'Rfq' then min_date end) as rfq,
  max(case when status = 'Rfq' then next_status_date end) as rfq_end,
  max(case when status = 'PreparingSalesQuote' then min_date end) as preparing_sales_quote,
  max(case when status = 'PreparingSalesQuote' then next_status_date end) as preparing_sales_quote_end,
  max(case when status = 'WaitingSalesQuoteFeedback' then min_date end) as waiting_sales_quote_feedback,
  max(case when status = 'WaitingSalesQuoteFeedback' then next_status_date end) as waiting_sales_quote_feedback_end,
  max(case when status = 'FormingOrder' then min_date end) as forming_order,
  max(case when status = 'FormingOrder' then next_status_date end) as forming_order_end,
  max(case when status = 'SigningAndWaitingForPayment' then min_date end) as signing_and_waiting_for_payment,
  max(case when status = 'SigningAndWaitingForPayment' then next_status_date end) as signing_and_waiting_for_payment_end,
  max(case when status = 'ManufacturingAndDelivery' then min_date end) as manufacturing_and_delivery,
  max(case when status = 'ManufacturingAndDelivery' then next_status_date end) as manufacturing_and_delivery_end,
  max(case when status = 'DealCompleted' then min_date end) as deal_completed
from 
(select deal_id, status, min_date, next_status, next_status_date, user_id, country, owner_email, owner_id, owner_role
from {{ ref('fact_deals_statuses_change') }}
left join (
  select distinct deal_id, user_id, country, owner_email, owner_id, owner_role from {{ ref('fact_deals') }}
  ) using (deal_id)
)
group by deal_id, 
  user_id,
  country,
  owner_email, owner_id, owner_role
