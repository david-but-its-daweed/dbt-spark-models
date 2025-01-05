{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'model_owner' : '@analytics.duty',
      'bigquery_load': 'true',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks'
    },
    tags=['data_readiness']
) }}

SELECT DISTINCT
    sds.slo_id,
    hap.full_table_name AS table_name,
    "bq" AS table_type
FROM {{ ref("slo_details") }} AS sds
INNER JOIN {{ ref('holistics_api_data') }} AS hap
    ON TRIM(sds.business_name) = TRIM(hap.dashboard_title)
WHERE
    hap.full_table_name IS NOT NULL
    AND NOT (sds.slo_id = "slo_istaff" AND hap.full_table_name = "onfy_mart.ads_spends")
    AND NOT hap.full_table_name = "platform_slo.slo_details"
    AND NOT hap.full_table_name = "models.slo_report_links"
    AND NOT hap.full_table_name = "platform_slo.data_readiness_summary"
    AND NOT hap.full_table_name = "platform_slo.slo_tables"
    AND NOT hap.full_table_name = "mart.focused_product"
    AND NOT hap.full_table_name = "mart.regions"
    AND NOT hap.full_table_name = "payments.country_filter"
UNION ALL
SELECT
    slo_id,
    table_name,
    table_type
FROM {{ ref("slo_tables_seed") }}
