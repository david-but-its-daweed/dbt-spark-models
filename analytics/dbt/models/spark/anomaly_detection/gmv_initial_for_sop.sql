{{ config(
    schema='anomaly_detection',
    materialized='table',
    meta = {
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'date',
      'predictor_dimensions': 'country',
      'predictor_value_column': 'gmv_initial',
      'predictor_prophet_seasonality_mode': 'multiplicative',
      'predictor_frequency': '1d',
      'predictor_num_forecast_points': '120'
    }
) }}
SELECT
    partition_date as date,
    shipping_country as country,
    sum(gmv_initial) as gmv_initial
FROM {{ source('mart', 'fact_order_2020') }}
WHERE partition_date >= date('{{ var("start_date_ymd") }}') - INTERVAL 2 years
    AND shipping_country IN ('RU', 'BY', 'UA', 
        'MD', 'PE', 'ZA', 'MX', 'ES', 'IT', 'KZ', 
        'FR', 'RO', 'NL', 'GE', 'MT', 'UK', 'AU', 
        'CH', 'DE', 'GB', 'IL', 'US', 'BE', 'SE',
        'AT', 'PL')
GROUP BY 1,2
