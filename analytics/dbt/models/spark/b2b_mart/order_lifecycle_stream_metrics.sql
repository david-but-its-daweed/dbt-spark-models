{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

with events as (SELECT
  order_id, 
  partition_date_msk,
  status, 
  sub_status,
  client_currency,
  total_final_price,
  ddp_final_price,
  dap_final_price,
  ewx_final_price,
  exw_final_price,
  commission_final_price,
  client_converted_gmv,
  final_gmv,
  final_gross_profit,
  initial_gross_profit,
  row_number() over (partition by order_id, status order by event_ts_msk desc) as last_status_value,
  row_number() over (partition by order_id order by event_ts_msk desc) as last_status
  from {{ ref('fact_order_change') }}
),

orders as (
  select 
  order_id, 
  delivery_time_days,
  created_ts_msk
  from {{ ref('fact_order') }}
),

shipping as (
  select 
  order_id, 
    linehaul_channel_type,
    manufacturing,
    shipping,
    days_in_shipping,
    days_in_delivered,
    days_in_manufacturing
    from {{ ref('sla_shipping_day') }}
)

select 
  e.order_id, 
  partition_date_msk,
  status, 
  sub_status,
  client_currency,
  total_final_price,
  ddp_final_price,
  dap_final_price,
  ewx_final_price,
  exw_final_price,
  commission_final_price,
  client_converted_gmv,
  final_gmv,
  final_gross_profit,
  initial_gross_profit,
  last_status,
  delivery_time_days,
  created_ts_msk,
  linehaul_channel_type,
    manufacturing,
    shipping,
    days_in_shipping,
    days_in_delivered,
    days_in_manufacturing
from events AS e
left join orders o on e.order_id = o.order_id
left join shipping s on s.order_id = e.order_id
where last_status_value = 1
