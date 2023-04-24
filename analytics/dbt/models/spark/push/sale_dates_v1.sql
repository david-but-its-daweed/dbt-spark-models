{{ 
  config(
    meta = {
      'model_owner' : '@gusev',
      'bigquery_load': 'true'
    }
  ) 
}}

SELECT * FROM {{ source('push', 'sale_dates') }}
