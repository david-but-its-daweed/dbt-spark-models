{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_dimensions': 'request_root_path',
      'predictor_value_column': 'max_requests_count',
      'predictor_frequency': '10min',
      'predictor_last_train_point': '{{ next_ds }}',
      'predictor_num_forecast_points': '0',
      'predictor_prophet_seasonality_mode': 'multiplicative',
    }
) }}
SELECT
    request_root_path,
    to_timestamp(from_unixtime(int(unix_timestamp(window) / 600) * 600)) t,
    max(requests_count) max_requests_count
FROM (
    SELECT
        request_root_path,
        from_unixtime(int(unix_timestamp(published_at) / 60) * 60) window,
        ip,
        count(1) as requests_count
    FROM threat.bot_slo_marketplace
    WHERE partition_date >= date('{{ var("start_date_ymd") }}') - INTERVAL 14 days AND NOT is_bot
    GROUP BY 1,2,3
)
GROUP BY 1,2
ORDER BY 1,2
