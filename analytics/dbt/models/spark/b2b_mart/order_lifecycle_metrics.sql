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
  linehaul_channel_id,
  created_ts_msk
  from {{ ref('fact_order') }}
),

stg1 AS (
    SELECT
        o.order_id,
        o.status,
        o.sub_status,
        o.event_ts_msk,
        CASE WHEN MAX(o.certification_final_price) OVER (PARTITION BY o.order_id) IS NOT NULL
            AND MAX(o.certification_final_price) OVER (PARTITION BY o.order_id) > 0 THEN 1 ELSE 0 END AS certified,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS min_sub_status_ts,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS min_status_ts,
        LEAD(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status_ts,
        LEAD(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status,
        LEAD(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status,
        LAG(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_sub_status_ts,
        LAG(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_status,
        LAG(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_sub_status
    FROM {{ ref('fact_order_change') }} AS o
),

stg2 AS (
    SELECT
        order_id,
        status,
        sub_status,
        event_ts_msk,
        current_status,
        current_sub_status,
        certified,
        IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL, TRUE, FALSE) AS flg,
        COALESCE(lead_sub_status_ts, CURRENT_TIMESTAMP()) AS lead_sub_status_ts,
        COALESCE(
            FIRST_VALUE(CASE WHEN lead_status != status THEN lead_sub_status_ts END)
            OVER (PARTITION BY order_id, status ORDER BY lead_status != status, lead_sub_status_ts),
            CURRENT_TIMESTAMP()) AS lead_status_ts,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status, sub_status
            ORDER BY event_ts_msk) AS first_substatus_event_msk,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status
            ORDER BY event_ts_msk) AS first_status_event_msk
    FROM stg1
    WHERE (lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL)
        OR (lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL)
),


orders_hist AS (
    SELECT
        order_id,
        current_status,
        current_sub_status,
        MAX(certified) AS certified,
        MAX(IF(status = 'manufacturing', DATEDIFF(DATE(lead_status_ts), DATE(first_status_event_msk)), NULL)) AS days_in_manufacturing,
        MAX(IF(status = 'shipping', DATEDIFF(DATE(lead_status_ts), DATE(first_status_event_msk)), NULL)) AS days_in_shipping,
        MAX(IF(sub_status = 'delivered', DATEDIFF(DATE(lead_sub_status_ts), DATE(first_substatus_event_msk)), NULL)) AS days_in_delivered,
        MAX(IF(status = 'manufacturing', first_status_event_msk, NULL)) AS manufacturing,
        MAX(IF(status = 'shipping', first_status_event_msk, NULL)) AS shipping,
        MAX(IF(status = 'shipping' AND sub_status = 'delivered', first_substatus_event_msk, NULL)) AS delivered
    FROM stg2
    WHERE flg = TRUE AND status IN ('manufacturing', 'shipping')
    GROUP BY order_id,
        current_status,
        current_sub_status
),

linehaul AS (
    SELECT DISTINCT
        id,
        name,
        min_days,
        max_days,
        channel_type
    FROM {{ ref('linehaul_channels') }}
),

shipping as (
  select 
  order_id, 
    manufacturing,
    shipping,
    days_in_shipping,
    days_in_delivered,
    days_in_manufacturing
    from orders_hist
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
  l.name as linehaul_name,
  l.channel_type as linehaul_channel_type,
  o.linehaul_channel_id,
  manufacturing,
  shipping,
  delivered,
  days_in_shipping,
  days_in_delivered,
  days_in_manufacturing
from events AS e
left join orders o on e.order_id = o.order_id
left join shipping s on s.order_id = e.order_id
left join linehaul l on o.linehaul_channel_id = l.id
where last_status_value = 1
