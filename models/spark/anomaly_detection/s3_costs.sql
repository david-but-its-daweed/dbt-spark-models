{{ config(
    schema='anomaly_detection',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@vladimir',
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'day',
      'predictor_dimensions': 'bucket',
      'predictor_value_column': 'cost',
      'predictor_prophet_seasonality_mode': 'multiplicative',
      'predictor_frequency': '1d',
      'predictor_num_forecast_points': '30',
      'anomalies_channel': '#anomalies-nobody-cares-about',
      'anomalies_significance_score': 'cumulative_deviation',
      'anomalies_significance_threshold': '0.2',
      'anomalies_max_count': '10',
      'anomalies_main_owners': ['@aleksandrov', '@aleksandrov'],
    },
    tags=['platform']
) }}
SELECT
    *
FROM {{ source('platform', 's3_costs') }}
