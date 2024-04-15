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

manufacturing AS
(
    select order_id, MIN(IF(status = 'manufacturing', event_ts_msk, NULL)) as min_manufactured_ts_msk
    from
    {{ ref('fact_order_statuses_change') }}
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
       case when payment_channel = 1 then 'Internet projects' else 'CIA' end as payment_channel,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_order') }} t
left join manufacturing m on t.order_id = m.order_id
WHERE t.order_id NOT IN ('660e4db2549ee70ee636f730') -- копия заказа, убираем, чтобы не дублировалось
