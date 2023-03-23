{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'predictor_enabled': 'true',
      'predictor_model': 'prophet_generic',
      'predictor_time_column': 'd,t',
      'predictor_dimensions': 'request_root_path',
      'predictor_value_column': 'max_requests_count',
      'predictor_frequency': '10min',
      'predictor_last_train_point': '{{ ds | plus_days(-1) }}',
      'predictor_num_forecast_points': '0',
      'predictor_prophet_seasonality_mode': 'multiplicative',
      'anomalies_channel': '#anomalies-automatic',
      'anomalies_start_time': '{{ ds | plus_days(-3) }}',
      'anomalies_end_time': '{{ ds | plus_days(-1) }}',
      'anomalies_significance_score': 'cumulative_deviation',
      'anomalies_significance_threshold': '8.0',
      'anomalies_group_by_time_column': 'd',
      'anomalies_plot_start_time': '{{ ds | plus_days(-5) }}',
      'anomalies_max_count': '10',
    }
) }}
SELECT
    request_root_path,
    to_date(from_unixtime(int(window / 86400) * 86400)) d,
    to_timestamp(from_unixtime(int(window / 600) * 600)) t,
    max(requests_count) max_requests_count
FROM (
    SELECT
        request_root_path,
        int(unix_timestamp(published_at) / 60) * 60 as window,
        ip,
        count(1) as requests_count
    FROM threat.bot_slo_marketplace
    WHERE partition_date >= date('{{ var("start_date_ymd") }}') - INTERVAL 14 days AND NOT is_bot
    GROUP BY 1,2,3
)
GROUP BY 1,2,3
ORDER BY 1,2,3
