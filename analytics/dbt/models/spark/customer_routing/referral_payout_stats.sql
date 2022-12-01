{{ config(
    schema='customer_routing',
    materialized='view',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}
SELECT *
FROM {{ source('ads', 'referral_payout_stats')}}
