{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH 
sub_statuses as (
    select 
    'Client2BrokerPaymentSent' as status,  2010 as pr
    union all
    select 
	'BrokerPaymentReceived', 2020 as pr
    union all
    select 
	'Broker2JoomSIAPaymentSent', 2030 as pr
    union all
    select 
	'JoomSIAPaymentReceived', 2049 as pr
                 ),
manufactiring AS
(
    select order_id, min(event_ts_msk) as min_manufactured_ts_msk
    from
    (
    select event_ts_msk, order_id, min(pr) over (partition by order_id) min_pr, pr
    from {{ ref('fact_order_change') }} foc left join sub_statuses s on foc.sub_status = s.status
    where foc.status = 'manufacturing'
    )
    where min_pr = pr or pr is null
    group by order_id
)

SELECT
       t.order_id,
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       ccy AS user_ccy,
       delivery_time_days,
       friendly_id,
       request_id,
       linehaul_channel_id,
       device_id,
       user_id,
       reject_reason,
       last_order_status,
       last_order_sub_status,
       m.min_manufactured_ts_msk,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_order') }} t
left join manufactiring m on t.order_id = m.order_id
