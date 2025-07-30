{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['effective_date'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'effective_date',
      'bigquery_overwrite': 'true',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

select 
    product_id,
    medicine.country_local_id as pzn,
    if(channel like 'Medizinfuchs%', 'Medizinfuchs', channel) as channel,
    date(effective_ts) as effective_date,
    min_by(pharmacy_name, product_price) as cheapest_competitor,
    min(product_price) as price
from {{ source('pharmacy', 'marketing_channel_price_fast_scd2') }}
join {{ source('pharmacy_landing', 'medicine') }}
    on marketing_channel_price_fast_scd2.product_id = medicine.id
where 1=1
    and product_price is not null
    and effective_ts >= '2023-01-01'
    and status = 'ACTIVE'
group by 
    product_id,
    medicine.country_local_id,
    if(channel like 'Medizinfuchs%', 'Medizinfuchs', channel),
    date(effective_ts)
distribute by 
    effective_date
