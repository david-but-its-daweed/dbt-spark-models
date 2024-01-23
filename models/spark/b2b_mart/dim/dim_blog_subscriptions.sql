{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


select
    _id as subscription_id,
    millis_to_ts_msk(ctms) as created_ts_msk,
    email,
    friendlyId as subscription_friendly_id
from {{ source('mongo', 'b2b_core_blog_subscriptions_daily_snapshot') }}
