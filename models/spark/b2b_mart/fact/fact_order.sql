{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH 
sub_statuses as (
    select 
    'client2BrokerPaymentSent' as status,  2010 as pr
    union all
    select 
	'brokerPaymentReceived', 2020 as pr
    union all
    select 
	'broker2JoomSIAPaymentSent', 2030 as pr
    union all
    select 
	'joomSIAPaymentReceived', 2049 as pr
                 ),
manufacturing AS
(
    select order_id, min(event_ts_msk) as min_manufactured_ts_msk
    from
    (
    select event_ts_msk, order_id, min(pr) over (partition by order_id, foc.status) min_pr, pr
    from {{ ref('fact_order_statuses_change') }} foc left join sub_statuses s on foc.sub_status = s.status
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
       owner_id,
       customs_id,
       logistician_id,
       biz_dev_id,
       lawyer_id,
       last_order_status,
       last_order_sub_status,
       m.min_manufactured_ts_msk,
       order_description,
       CASE WHEN delivery_scheme = 0 THEN 'DAP' WHEN delivery_scheme = 1 THEN 'EXW' END AS delivery_scheme,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_order') }} t
left join manufacturing m on t.order_id = m.order_id
