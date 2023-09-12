{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@aplotnikov',
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'ts',
      'predictor_dimensions': 'isp,user_agent',
      'predictor_value_column': 'unique_querying_devices',
      'predictor_frequency': '5min',
      'predictor_last_train_point': '{{ ds }}',
      'predictor_num_forecast_points': '0',
      'predictor_prophet_seasonality_mode': 'multiplicative',
    }
) }}
select
    isp,
    user_agent,
    ts,
    unique_querying_devices
from {{ ref('onfy_unique_querying_devices') }}
where
    frequency = '5m'
    and isp is null
    and user_agent is null
    and ts >= timestamp('{{ var("start_date_ymd") }}') - interval 15 days -- using 15 days to capture weekly seasonality
order by ts