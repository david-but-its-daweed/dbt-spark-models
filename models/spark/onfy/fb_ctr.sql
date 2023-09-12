{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}


with fb as (
     select
         campaign_name,
         'facebook' as source,
         date_trunc('day', date) as date,
         sum(impressions) as impressions,
         sum(spend) as spend,
         sum(clicks) as clicks,
         sum(clicks) / sum(impressions) as ctr,
         sum(spend) / sum(clicks) as cpc
     from
        {{ source('pharmacy', 'fbads_adset_country_insights') }}
     where 1=1
        and date >= '2022-01-01'
     group by 
        campaign_name,
        date_trunc('day', date)
)
 
select *
from fb
