{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    },
    tags=['data_readiness']
) }}


select slo_id, full_table_name as table_name, "bq" as table_type
from {{ref("slo_details_seed")}}
         inner join platform.holistics_dashboards on trim(business_name) = trim(dashboard_title)
where full_table_name is not null
and not( slo_id = 'slo_istaff' and full_table_name = 'onfy_mart.ads_spends')
and not full_table_name = 'platform_slo.slo_details'
and not full_table_name = 'models.slo_report_links'
and not full_table_name = 'platform_slo.data_readiness_summary'
and not full_table_name = 'platform_slo.slo_tables'
and not full_table_name = 'mart.focused_product'
and not full_table_name = 'mart.regions'
and not full_table_name = 'payments.country_filter'
    UNION ALL
SELECT slo_id, table_name, table_type
FROM {{ref("slo_tables_seed")}}
