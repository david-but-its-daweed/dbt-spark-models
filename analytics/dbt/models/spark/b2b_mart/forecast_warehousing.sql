{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}


with 
days as (

select 'Auto' as linehaul_channel, 'clientPaymentSent' as status, 1001 as status_int, 4 as days
union all 
select 'Auto' as linehaul_channel, 'advancePaymentRequested' as status, 1010 as status_int, 4 as days
union all 
select 'Auto' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 1020 as status_int, 0 as days
union all 
select 'Auto' as linehaul_channel, 'PSI' as status, 2010 as status_int, 6 as days
union all 
select 'Auto' as linehaul_channel, 'remainingPaymentRequested' as status, 2030 as status_int, 4 as days
union all 
select 'Auto' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3001 as status_int, 2 as days
union all 
select 'Auto' as linehaul_channel, 'pickedUpByLogisticians' as status, 3010 as status_int, 7 as days
union all 
select 'Auto' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 4001 as status_int, 7 as days
union all 
select 'Auto' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 5001 as status_int, 5 as days
union all 
select 'Aero' as linehaul_channel, 'clientPaymentSent' as status, 1001 as status_int, 4 as days
union all 
select 'Aero' as linehaul_channel, 'advancePaymentRequested' as status, 1010 as status_int, 4 as days
union all 
select 'Aero' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 1020 as status_int, 0 as days
union all 
select 'Aero' as linehaul_channel, 'PSI' as status, 2010 as status_int, 6 as days
union all 
select 'Aero' as linehaul_channel, 'remainingPaymentRequested' as status, 2030 as status_int, 4 as days
union all 
select 'Aero' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3001 as status_int, 2 as days
union all 
select 'Aero' as linehaul_channel, 'pickedUpByLogisticians' as status, 3010 as status_int, 7 as days
union all 
select 'Aero' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 4001 as status_int, 6 as days
union all 
select 'Aero' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 5001 as status_int, 2 as days
union all 
select 'Sea' as linehaul_channel, 'clientPaymentSent' as status, 1001 as status_int, 4 as days
union all 
select 'Sea' as linehaul_channel, 'advancePaymentRequested' as status, 1010 as status_int, 4 as days
union all 
select 'Sea' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 1020 as status_int, 0 as days
union all 
select 'Sea' as linehaul_channel, 'PSI' as status, 2010 as status_int, 6 as days
union all 
select 'Sea' as linehaul_channel, 'remainingPaymentRequested' as status, 2030 as status_int, 4 as days
union all 
select 'Sea' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3001 as status_int, 5 as days
union all 
select 'Sea' as linehaul_channel, 'pickedUpByLogisticians' as status, 3010 as status_int, 5 as days
union all 
select 'Sea' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 4001 as status_int, 10 as days
union all 
select 'Sea' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 5001 as status_int, 21 as days
union all 
select 'Rail' as linehaul_channel, 'clientPaymentSent' as status, 1001 as status_int, 4 as days
union all 
select 'Rail' as linehaul_channel, 'advancePaymentRequested' as status, 1010 as status_int, 4 as days
union all 
select 'Rail' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 1020 as status_int, 0 as days
union all 
select 'Rail' as linehaul_channel, 'PSI' as status, 2010 as status_int, 6 as days
union all 
select 'Rail' as linehaul_channel, 'remainingPaymentRequested' as status, 2030 as status_int, 4 as days
union all 
select 'Rail' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3001 as status_int, 2 as days
union all 
select 'Rail' as linehaul_channel, 'pickedUpByLogisticians' as status, 3010 as status_int, 7 as days
union all 
select 'Rail' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 4001 as status_int, 13 as days
union all 
select 'Rail' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 5001 as status_int, 23 as days

),


pickups as 
(
    select distinct
        boxes.operationalProductId as product_id,
        pickup_id,
        pickup_friendly_id,
        order_id,
        merchant_order_id,
        arrived_date,
        pickup_date,
        planned_date,
        shipped_date
    from
    (
    select 
        explode(boxes) as boxes,
        _id as pickup_id,
        friendlyId as pickup_friendly_id,
        orderId as order_id,
        merchOrdId as merchant_order_id,
        date(millis_to_ts_msk(arrivedDate)) as arrived_date,
        date(millis_to_ts_msk(pickUpDate)) as pickup_date,
        date(millis_to_ts_msk(plannedDate)) as planned_date,
        date(millis_to_ts_msk(shippedDate)) as shipped_date
    from {{ ref('scd2_pick_up_orders_snapshot') }}
    where dbt_valid_to is null
    )
),


orders as (
    select user_id, fo.order_id, friendly_id,
        fo.created_ts_msk as partition_date_msk,
        min_manufactured_ts_msk as min_manufacturing_time,
        channel_type
    from {{ ref('fact_order') }} fo
    left join {{ ref('linehaul_channels') }} on linehaul_channel_id = id
    where next_effective_ts_msk is null
),

merchant_orders as (
    select distinct 
        order_id, 
        merchant_order_id,
        manufacturing_days,
        friendly_id as merchant_order_friendly_id
    from {{ ref('fact_merchant_order') }}
    where next_effective_ts_msk is null
),
    

boxes as (
select merchant_order_id,
    product_id,
    length,
    width,
    hight, 
    weight,
    qty,
    qty_per_box,
    length*width*hight/1000000 as measures
    from
(
select
    merchOrdId as merchant_order_id,
    product_id,
    length as length,
    value.operationalProductId[n] as operational_product_id,
    (value.w)[n] as width,
    value.h[n] as hight, 
    value.weight[n] as weight,
    value.qty[n] as qty,
    value.qtyPerBox[n] as qty_per_box

from
    (
    select merchOrdId, explode(phases), product_id, ctms
    from
    (
    select ctms, merchOrdId, packaging.*, id as product_id
    from
    {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
    order by ctms desc
    )
    ) a
lateral view posexplode(value.l) n as n, length
)
),


dict as (
    select distinct
        o.order_id,
        o.friendly_id as order_friendly_id,
        o.channel_type,
        o.partition_date_msk as order_created_time,
        o.min_manufacturing_time,
        mo.merchant_order_id,
        mo.merchant_order_friendly_id,
        manufacturing_days,
        b.product_id,
        pickup_id,
        pickup_friendly_id
    from orders o 
    left join merchant_orders mo on o.order_id = mo.order_id
    left join boxes b on b.merchant_order_id = mo.merchant_order_id
    left join pickups p on o.order_id = p.order_id and mo.merchant_order_id = p.merchant_order_id
    and p.product_id = b.product_id
    where o.min_manufacturing_time >= '2022-07-01' and b.product_id is not null
    ),
    

order_statuses as (
select 
    d.*,
    date_status,
    status, 
    status_int,
    1 as priority
from
    (
    select 
        order_id,
        
        date_status,
        
        case when sub_status = 'client2BrokerPaymentSent' then 'clientPaymentSent'
        when sub_status = 'pickupRequestSentToLogisticians' then 'pickupRequestSentToLogisticians'
        when sub_status = 'pickedUpByLogisticians' then 'pickedUpByLogisticians'
        when sub_status = 'arrivedAtLogisticsWarehouse' then 'arrivedAtLogisticsWarehouse'
        when sub_status = 'departedFromLogisticsWarehouse' then 'departedFromLogisticsWarehouse' end as status,
        
        case when sub_status = 'client2BrokerPaymentSent' then 1001
        when sub_status = 'pickupRequestSentToLogisticians' then 3001
        when sub_status = 'pickedUpByLogisticians' then 3010
        when sub_status = 'arrivedAtLogisticsWarehouse' then 4001
        when sub_status = 'departedFromLogisticsWarehouse' then 5001 end as status_int
        from
    (
        select distinct
            order_id,
            date(event_ts_msk) as date_status,
            status,
            sub_status
        from {{ ref('fact_order_statuses') }}
        where status in (
            'manufacturing',
            'shipping'
            )
        and sub_status in (
            'client2BrokerPaymentSent',
            'joomSIAPaymentReceived',
            'pickedUpByLogisticians',
            'arrivedAtLogisticsWarehouse',   
            'pickupRequestSentToLogisticians',
            'departedFromLogisticsWarehouse'
            )
        order by status
    )
    ) o
    join dict d on o.order_id = d.order_id
    where status is not null and date_status is not null
),

merchant_order_statuses as (
    select 
        d.*,
        date_status,
        status, 
        status_int,
        2 as priority
    from
    (
    select 
        merchant_order_id,
        
        date(day) as date_status,
        
        case when status = 'advancePaymentRequested' then 'advancePaymentRequested'
        when status = 'manufacturingAndQcInProgress' then 'manufacturingAndQcInProgress'
        when status = 'remainingPaymentRequested' then 'remainingPaymentRequested' end as status,
        
        case when status = 'advancePaymentRequested' then 1010
        when status = 'manufacturingAndQcInProgress' then 1020
        when status = 'remainingPaymentRequested' then 2030 end as status_int
        from
    (
    select payload.id as merchant_order_id,
            payload.status,
            min(TIMESTAMP(millis_to_ts_msk(payload.updatedTime))) as day
            from {{ source('b2b_mart', 'operational_events') }}
            WHERE type  ='merchantOrderChanged'
            and payload.status in ('advancePaymentRequested', 'manufacturingAndQcInProgress', 'remainingPaymentRequested')
            group by payload.id,
            payload.status
    )
    ) o join dict d on o.merchant_order_id = d.merchant_order_id
    where status is not null and date_status is not null
),

product_statuses as (
select 
        d.*,
        date_status,
        status, 
        status_int,
        3 as priority
    from
(
select distinct 
    merchant_order_id,
    product_id,
    date_status,
    case when status = 'readyForPsi' then 'PSI'
        when status = 'pickupRequested' then 'pickupRequestSentToLogisticians'
        when status = 'pickupCompleted' then 'pickedUpByLogisticians' end as status,
    case when status = 'readyForPsi' then 2010
        when status = 'pickupRequested' then 3001
        when status = 'pickupCompleted' then 3010 end as status_int
    from
    (
      select merchant_order_id,
        product_id, status, date(min(event_ts_msk)) as date_status
    from {{ ref('statuses_events') }}
        where entity = 'product'
        group by merchant_order_id,
        product_id, status
    )
) o join dict d on o.merchant_order_id = d.merchant_order_id and o.product_id = d.product_id
    where status is not null and date_status is not null
),

pickup_statuses as 
(select 
        d.*,
        date_status,
        status, 
        status_int,
        3 as priority
    from
(
    select 
    case when status = 'WaitingForConfirmation' then 'pickupRequestSentToLogisticians'
        when status = 'Requested' then 'pickupRequestSentToLogisticians'
        end as status,
    case when status = 'WaitingForConfirmation' then 3001
        when status = 'Requested' then 3002
        end as status_int,
        time as date_status,
        pickup_id
        from
    (
        select 
        col.status as status,
        date(millis_to_ts_msk(col.updatedTimeMs)) as time,
        pickup_id
        from
        (
        select distinct 
        _id as pickup_id,
        explode(state.statusHistory)
        from {{ ref('scd2_pick_up_orders_snapshot') }}
        where dbt_valid_to is null
        )
        )
    union all
    select 
        'pickupCompleted' as status,
        3010 as status_int,
        planned_date as date_status,
        pickup_id
    from pickups
    union all 
    select 
        'pickupCompleted' as status,
        3011 as status_int,
        shipped_date as date_status,
        pickup_id
    from pickups
) o join dict d on o.pickup_id = d.pickup_id 
    where status is not null and date_status is not null
),

psi as (
select 
        d.*,
        date_status,
        status, 
        status_int,
        4 as priority
        from
(
    select  
        max(psi_start) as date_status,
        'PSI' as status,
        2010 as status_int,
        product_id,
        merchant_order_id
    from {{ ref('fact_psi') }}
    group by 
        product_id,
        merchant_order_id
    ) o 
    join dict d on o.merchant_order_id = d.merchant_order_id and o.product_id = d.product_id
    where status is not null and date_status is not null
),

all as (
select 
    a.*,
    b.length,
    b.width,
    b.hight, 
    b.weight,
    b.qty,
    b.qty_per_box,
    length*width*hight/1000000 as measures
    from
(select 
    order_id,
    order_friendly_id,
    channel_type,
    order_created_time,
    min_manufacturing_time,
    merchant_order_id,
    merchant_order_friendly_id,
    manufacturing_days,
    product_id,
    pickup_id,
    pickup_friendly_id,
    status,
    status_int,
    date_status
from
(
select * , 
row_number() over (partition by order_id, merchant_order_id, product_id, pickup_id order by status_int desc, priority desc) as rn
from
(
select * from 
order_statuses 
union all 
select * from 
merchant_order_statuses
union all 
select * from 
product_statuses
union all 
select * from 
pickup_statuses
union all 
select * from 
psi
)
)
where rn = 1
) a 
left join boxes b on a.merchant_order_id = b.merchant_order_id and a.product_id = b.product_id
)


select distinct
    order_id,
    order_friendly_id,
    channel_type,
    order_created_time,
    min_manufacturing_time,
    merchant_order_id,
    merchant_order_friendly_id,
    manufacturing_days,
    product_id,
    pickup_id,
    pickup_friendly_id,
    status,
    status_int,
    date_status,
    length,
    width,
    hight, 
    weight,
    qty,
    qty_per_box,
    measures,
    sum(days) over (partition by order_id, merchant_order_id, product_id, pickup_id, measures, qty_per_box) as day_diff,
    date('{{ var("start_date_ymd") }}') as partition_date_msk
from 
(
select distinct
    all.*,
    case when all.status_int = days.status_int then greatest(datediff(current_date, date_status), days)
    when all.status_int = 1020 and all.status_int = days.status_int then greatest(datediff(current_date, date_status), manufacturing_days)
    when all.status_int >= 4001 then 0
    else days end as days
from all left join days on all.channel_type = days.linehaul_channel and all.status_int <= days.status_int
)
