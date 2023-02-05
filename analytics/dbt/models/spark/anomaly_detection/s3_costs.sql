{{ config(
    schema='anomaly_detection',
    materialized='table',
    meta = {
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'day',
      'predictor_dimensions': 'bucket',
      'predictor_value_column': 'cost',
      'predictor_prophet_seasonality_mode': 'multiplicative',
      'predictor_frequency': '1d',
      'predictor_num_forecast_points': '120'
    }
) }}
SELECT
    *
FROM {{ source('platform', 's3_costs') }}
