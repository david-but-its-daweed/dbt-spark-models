{{ config(
    schema='anomaly_detection',
    materialized='table',
    meta = {
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'day',
      'predictor_dimensions': 'service',
      'predictor_value_column': 'cost',
      'predictor_prophet_seasonality_mode': 'multiplicative',
      'predictor_frequency': '1d',
      'predictor_num_forecast_points': '30',
      'anomalies_channel': '#anomalies-automatic',
      'anomalies_significance_score': 'cumulative_deviation',
      'anomalies_significance_threshold': '0.2',
      'anomalies_max_count': '10',
    },
    tags=['platform']
) }}
SELECT
    *
FROM {{ source('platform', 'aws_service_costs') }}
