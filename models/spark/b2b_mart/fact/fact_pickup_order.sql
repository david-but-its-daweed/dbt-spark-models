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

with statuses as (
  select 'WaitingForConfirmation' as status, 2 as value
  union all 
  select 'Requested' as status, 5 as value
  union all 
  select 'Approved' as status, 10 as value
  union all 
  select 'PickedUp' as status, 13 as value
  union all 
  select 'Arrived' as status, 16 as value
  union all 
  select 'Shipped' as status, 20 as value
  union all 
  select 'Suspended' as status, 30 as value
  )

select 
  pickup_id,
  pickup_friendly_id,
  merchant_order_id,
  order_id,
  arrived_date,
  pickup_date,
  planned_date,
  shipped_date,
  min(case when statuses.status = 'WaitingForConfirmation' then time end) as waiting_for_confirmation,
  min(case when statuses.status = 'Requested' then time end) as requested,
  min(case when statuses.status = 'Approved' then time end) as approved,
  min(case when statuses.status = 'PickedUp' then time end) as picked_up,
  min(case when statuses.status = 'Arrived' then time end) as arrived,
  min(case when statuses.status = 'Shipped' then time end) as shipped,
  min(case when statuses.status = 'Suspended' then time end) as suspended,
  min(case when statuses.status = 'WaitingForConfirmation' then lead_time end) as waiting_for_confirmation_end,
  min(case when statuses.status = 'Requested' then lead_time end) as requested_end,
  min(case when statuses.status = 'Approved' then lead_time end) as approved_end,
  min(case when statuses.status = 'PickedUp' then lead_time end) as picked_up_end,
  min(case when statuses.status = 'Arrived' then lead_time end) as arrived_end,
  min(case when statuses.status = 'Shipped' then lead_time end) as shipped_end,
  min(case when statuses.status = 'Suspended' then lead_time end) as suspended_end,
  max(case when rn = 1 then statuses.status end) as current_status
from
(
select 
pickup_id,
pickup_friendly_id,
merchant_order_id,
order_id,
arrived_date,
pickup_date,
planned_date,
shipped_date,
col.status as status,
millis_to_ts_msk(col.updatedTimeMs) as time,
lead(millis_to_ts_msk(col.updatedTimeMs)) over (partition by pickup_id order by millis_to_ts_msk(col.updatedTimeMs)) as lead_time,
row_number() over (partition by pickup_id order by col.updatedTimeMs desc) as rn
from
(
select 
_id as pickup_id,
friendlyId as pickup_friendly_id,
merchOrdId as merchant_order_id,
orderId as order_id,
millis_to_ts_msk(arrivedDate) as arrived_date,
millis_to_ts_msk(pickUpDate) as pickup_date,
millis_to_ts_msk(plannedDate) as planned_date,
millis_to_ts_msk(shippedDate) as shipped_date,
explode(state.statusHistory)
from {{ ref('scd2_pick_up_orders_snapshot') }}
)
) a
left join statuses on a.status = value
group by pickup_id,
  pickup_friendly_id,
  merchant_order_id,
  order_id,
  arrived_date,
  pickup_date,
  planned_date,
  shipped_date
