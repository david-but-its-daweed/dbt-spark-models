{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@aplotnikov',
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'partition_date_msk',
      'predictor_dimensions': 'event_type,device_country,os_type',
      'predictor_value_column': 'event_count',
      'predictor_frequency': '1d',
      'predictor_prophet_seasonality_mode': 'multiplicative',
      'anomalies_channel': '#anomalies-nobody-cares-about',
      'anomalies_significance_score': 'cumulative_deviation',
      'anomalies_significance_threshold': '1.0',
      'anomalies_max_count': '10',
    }
) }}
select
    event_type,
    device_country,
    os_type,
    partition_date_msk,
    event_count
from {{ source('cube', 'event_counts_country_os_days') }}
left anti join {{ source('platform', 'fact_removed_device_event') }} using (event_type)
where
    time_unit = 'd'
    and (device_country = 'all' or event_type = 'all')
order by partition_date_msk
