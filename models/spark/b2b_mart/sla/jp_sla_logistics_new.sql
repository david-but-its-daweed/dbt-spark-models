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

with stages as (
  select * from {{ ref('key_sla_stage') }}
),

orders AS (
  SELECT order_id,
    friendly_id,
    created_day,
    created_ts_msk,
    linehaul_channel_id,
    delivery_time_days,
    linehaul_channel_name,
    linehaul_min_days,
    linehaul_max_days,
    linehaul_channel_type,
    manufacturing_days,
    manufacturing,
    shipping,
    current_status,
    current_sub_status
  FROM {{ ref('jp_sla_logistics') }}
)

select
        _id as sla_id,
        entityId as order_id,
        friendly_id,
        created_day,
        created_ts_msk,
        linehaul_channel_id,
        delivery_time_days,
        linehaul_channel_name,
        linehaul_channel_type,
        manufacturing_days,
        to_date(cast(cast(actualDate as int) as string), 'yyyyMMdd') as actual_date,
        to_date(cast(cast(plannedDate as int) as string), 'yyyyMMdd') as planned_date,
        to_date(cast(cast(slaDate as int) as string), 'yyyyMMdd') as sla_date,
        to_date(cast(cast(startDate as int) as string), 'yyyyMMdd') as start_date,
        comment,
        expectedDurationInDays as expected_duration,
        stage_int as stage_id,
        stage as status,
        manufacturing,
        shipping,
        current_status,
        current_sub_status
    from
    (
    select _id, entityId, entityType, statuses.*, startDate
    from
    (
    select _id, entityId, entityType, explode(slaTable) as statuses, startDate
    from
    {{ source('mongo', 'b2b_core_sla_trackers_daily_snapshot') }}
    )
    )
  LEFT JOIN stages ON CAST(stageID AS INT) = CAST(stage_int AS INT)
  LEFT JOIN orders ON entityId = order_id
