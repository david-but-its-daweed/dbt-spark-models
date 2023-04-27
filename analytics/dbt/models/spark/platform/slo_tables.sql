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
         inner join platform.holistics_dashboards on business_name = dashboard_title
where full_table_name is not null
    UNION ALL
SELECT slo_id, table_name, table_type
FROM {{ref("slo_tables_seed")}}
