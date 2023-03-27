{{ config(
    schema='b2b_mart',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false',
      'priority_weight': '150'
    }
) }}



with times as (
    select order_id, max(TIMESTAMP(millis_to_ts_msk(statuses.updatedTime))) as event_ts_msk
    from
    (
    SELECT  payload.orderId AS order_id,
        payload.status AS status,
        payload.subStatus AS sub_status,
        explode(payload.statusHistory) as statuses
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE type  ='orderChangedByAdmin'
      {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
       and partition_date   >= date'2022-05-19'
     {% endif %}
    and payload.reason = 'statusChanged'
      )
      where status = statuses.status and coalesce(sub_status, '') = coalesce(statuses.subStatus, '')
      and statuses.updatedTime <= 32511074144000 and statuses.updatedTime is not null
      group by order_id
)



SELECT a.event_id,
        partition_date_msk AS partition_date_msk,
        TIMESTAMP(coalesce(t.event_ts_msk, a.event_ts_msk)) AS event_ts_msk,
        a.order_id,
        client_currency,
        reason ,
        TIMESTAMP(coalesce(millis_to_ts_msk(owner_time), a.event_ts_msk)) AS owner_time_msk,
        owner_type,
        owner_moderator_id,
        owner_role_type,
        TIMESTAMP(coalesce(millis_to_ts_msk(biz_dev_time), a.event_ts_msk)) AS biz_dev_time_msk,
        biz_dev_type,
        biz_dev_moderator_id,
        biz_dev_role_type,
        status,
        sub_status,
        SUM(case when col.tag = 'grant' then -col.stagedPrices.final else col.stagedPrices.final end) AS total_final_price,
        SUM(IF(col.tag = 'ddp', col.stagedPrices.final, 0)) AS ddp_final_price,
        SUM(IF(col.tag = 'dap', col.stagedPrices.final, 0)) AS dap_final_price,
        SUM(IF(col.tag = 'ewx', col.stagedPrices.final, 0)) AS ewx_final_price,
        SUM(IF(col.tag = 'exw', col.stagedPrices.final, 0)) AS exw_final_price,
        SUM(IF(col.type = 'firstmileBeforeQC', col.stagedPrices.final, 0)) AS firstmile_before_qc_final_price,
        SUM(IF(col.type = 'warehousingOp', col.stagedPrices.final, 0)) AS warehousing_op_final_price,
        SUM(IF(col.type = 'firstmileAfterQC', col.stagedPrices.final, 0)) AS firstmile_after_qc_final_price,
        SUM(IF(col.type = 'utilityFee', col.stagedPrices.final, 0)) AS utility_fee_final_price,
        SUM(IF(col.type = 'customsFee', col.stagedPrices.final, 0)) AS customs_fee_final_price,
        SUM(IF(col.type = 'loadingUnloading', col.stagedPrices.final, 0)) AS loading_unloading_final_price,
        SUM(IF(col.type = 'labeling', col.stagedPrices.final, 0)) AS labeling_final_price,
        SUM(IF(col.type = 'commission', col.stagedPrices.final, 0)) AS commission_final_price,
        SUM(IF(col.type = 'merchantFee', col.stagedPrices.final, 0)) AS merchant_fee_final_price,
        SUM(IF(col.type = 'warehousing', col.stagedPrices.final, 0)) AS warehousing_final_price,
        SUM(IF(col.type = 'customsDuty', col.stagedPrices.final, 0)) AS customs_duty_final_price,
        SUM(IF(col.type = 'brokerFee', col.stagedPrices.final, 0)) AS broker_fee_final_price,
        SUM(IF(col.type = 'linehaul', col.stagedPrices.final,  0)) AS linehaul_final_price,
        SUM(IF(col.type = 'lastMile', col.stagedPrices.final, 0)) AS last_mile_final_price,
        SUM(IF(col.type = 'qc', col.stagedPrices.final,  0)) AS qc_final_price,
        SUM(IF(col.type = 'agentFee', col.stagedPrices.final,  0)) AS agent_fee_final_price,
        SUM(IF(col.type = 'certification', col.stagedPrices.final,  0)) AS certification_final_price,
        SUM(IF(col.type = 'vat', col.stagedPrices.final,  0)) AS vat_final_price,
        SUM(IF(col.type = 'generalCargo',col.stagedPrices.final,  0)) AS general_cargo_final_price,
        SUM(case when col.tag = 'grant' then -col.stagedPrices.confirmed else col.stagedPrices.confirmed end) AS total_confirmed_price,
        SUM(IF(col.tag = 'ddp',  col.stagedPrices.confirmed, 0)) AS ddp_confirmed_price,
        SUM(IF(col.tag = 'dap', col.stagedPrices.confirmed, 0)) AS dap_confirmed_price,
        SUM(IF(col.tag = 'ewx',  col.stagedPrices.confirmed, 0)) AS ewx_confirmed_price,
        SUM(IF(col.tag = 'exw', col.stagedPrices.confirmed, 0)) AS exw_confirmed_price,
        SUM(IF(col.type = 'firstmileBeforeQC', col.stagedPrices.confirmed, 0)) AS firstmile_before_qc_confirmed_price,
        SUM(IF(col.type = 'warehousingOp', col.stagedPrices.confirmed, 0)) AS warehousing_op_confirmed_price,
        SUM(IF(col.type = 'firstmileAfterQC', col.stagedPrices.confirmed, 0)) AS firstmile_after_qc_confirmed_price,
        SUM(IF(col.type = 'utilityFee',  col.stagedPrices.confirmed, 0)) AS utility_fee_confirmed_price,
        SUM(IF(col.type = 'customsFee',  col.stagedPrices.confirmed, 0)) AS customs_fee_confirmed_price,
        SUM(IF(col.type = 'loadingUnloading',col.stagedPrices.confirmed, 0)) AS loading_unloading_confirmed_price,
        SUM(IF(col.type = 'labeling',  col.stagedPrices.confirmed, 0)) AS labeling_confirmed_price,
        SUM(IF(col.type = 'commission', col.stagedPrices.confirmed, 0)) AS commission_confirmed_price,
        SUM(IF(col.type = 'merchantFee', col.stagedPrices.confirmed, 0)) AS merchant_fee_confirmed_price,
        SUM(IF(col.type = 'warehousing', col.stagedPrices.confirmed, 0)) AS warehousing_confirmed_price,
        SUM(IF(col.type = 'customsDuty', col.stagedPrices.confirmed, 0)) AS customs_duty_confirmed_price,
        SUM(IF(col.type = 'brokerFee', col.stagedPrices.confirmed, 0)) AS broker_fee_confirmed_price,
        SUM(IF(col.type = 'linehaul', col.stagedPrices.confirmed, 0)) AS linehaul_confirmed_price,
        SUM(IF(col.type = 'lastMile', col.stagedPrices.confirmed, 0)) AS lastMile_confirmed_price,
        SUM(IF(col.type = 'qc', col.stagedPrices.confirmed, 0)) AS qc_confirmed_price,
        SUM(IF(col.type = 'agentFee', col.stagedPrices.confirmed, 0)) AS agent_fee_confirmed_price,
        SUM(IF(col.type = 'certification', col.stagedPrices.confirmed, 0)) AS certification_confirmed_price,
        SUM(IF(col.type = 'vat', col.stagedPrices.confirmed, 0)) AS vat_confirmed_price,
        SUM(IF(col.type = 'generalCargo',col.stagedPrices.confirmed, 0)) AS general_cargo_confirmed_price,
        MAX(client_converted_gmv) AS client_converted_gmv,
        MAX(final_gmv) AS final_gmv,
        MAX(final_gross_profit) AS final_gross_profit,
        MAX(initial_gross_profit) AS initial_gross_profit,
        to_timestamp(a.event_ts_msk) AS ctms
FROM (
SELECT  event_id,
        partition_date AS partition_date_msk,
        coalesce(TIMESTAMP(millis_to_ts_msk(payload.updatedTime)), event_ts_msk) as event_ts_msk,
        payload.orderId AS order_id,
        payload.clientCurrency AS client_currency,
        posexplode_outer(payload.typedPrices),
        payload.reason AS reason,
        payload.roleSet.roles.owner.actualisationTime.time as owner_time,
        payload.roleSet.roles.owner.actualisationTime.type as owner_type,
        payload.roleSet.roles.owner.moderatorId as owner_moderator_id,
        payload.roleSet.roles.owner.roleType as owner_role_type,
        payload.roleSet.roles.bizDev.actualisationTime.time as biz_dev_time,
        payload.roleSet.roles.bizDev.actualisationTime.type as biz_dev_type,
        payload.roleSet.roles.bizDev.moderatorId as biz_dev_moderator_id,
        payload.roleSet.roles.bizDev.roleType as biz_dev_role_type,
        payload.status AS status,
        payload.subStatus AS sub_status,
        payload.gmv.clientConvertedGMV AS client_converted_gmv,
        payload.gmv.finalGMV AS final_gmv,
        payload.gmv.finalGrossProfit AS final_gross_profit,
        payload.gmv.initialGrossProfit AS initial_gross_profit
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE type  ='orderChangedByAdmin'
      {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
    and partition_date   >= date'2022-05-19'
     {% endif %}
    UNION ALL
    SELECT  event_id,
        partition_date  AS partition_date_msk,
        coalesce(TIMESTAMP(millis_to_ts_msk(payload.updatedTime)), event_ts_msk) as event_ts_msk,
        payload.orderId AS order_id,
        payload.clientCurrency AS client_currency,
        posexplode_outer(payload.otherPrices),
        payload.reason AS reason,
        payload.roleSet.roles.owner.actualisationTime.time as owner_time,
        payload.roleSet.roles.owner.actualisationTime.type as owner_type,
        payload.roleSet.roles.owner.moderatorId as owner_moderator_id,
        payload.roleSet.roles.owner.roleType as owner_role_type,
        payload.roleSet.roles.bizDev.actualisationTime.time as biz_dev_time,
        payload.roleSet.roles.bizDev.actualisationTime.type as biz_dev_type,
        payload.roleSet.roles.bizDev.moderatorId as biz_dev_moderator_id,
        payload.roleSet.roles.bizDev.roleType as biz_dev_role_type,
        payload.status AS status,
        payload.subStatus AS sub_status,
        payload.gmv.clientConvertedGMV AS client_converted_gmv,
        payload.gmv.finalGMV AS final_gmv,
        payload.gmv.finalGrossProfit AS final_gross_profit,
        payload.gmv.initialGrossProfit AS initial_gross_profit
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE type  ='orderChangedByAdmin'
      {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
       and partition_date   >= date'2022-05-19'
     {% endif %}
) a 
left join times t on a.order_id = t.order_id
GROUP BY a.event_id,
        partition_date_msk,
        a.event_ts_msk,
        t.event_ts_msk,
        a.order_id,
        client_currency,
        reason,
        owner_time,
        owner_type,
        owner_moderator_id,
        owner_role_type,
        biz_dev_time,
        biz_dev_type,
        biz_dev_moderator_id,
        biz_dev_role_type,
        status,
        sub_status
