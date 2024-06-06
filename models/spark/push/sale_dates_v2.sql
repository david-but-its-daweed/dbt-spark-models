{{ 
  config(
    schema='push',
    alias='sale_dates_v2',
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
  ) 
}}

SELECT * FROM {{ source('push', 'sale_dates_v1') }}
