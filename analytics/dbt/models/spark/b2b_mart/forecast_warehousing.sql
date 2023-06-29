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

select 'Auto' as linehaul_channel, 'clientPaymentSent' as status, 2010 as status_int, 4 as days
union all 
select 'Auto' as linehaul_channel, 'advancePaymentRequested' as status, 2050 as status_int, 4 as days
union all 
select 'Auto' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 2060 as status_int, 0 as days
union all 
select 'Auto' as linehaul_channel, 'PSI' as status, 2070 as status_int, 6 as days
union all 
select 'Auto' as linehaul_channel, 'remainingPaymentRequested' as status, 2080 as status_int, 4 as days
union all 
select 'Auto' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 2 as days
union all 
select 'Auto' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 7 as days
union all 
select 'Auto' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 7 as days
union all 
select 'Auto' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 5 as days
union all 
select 'Auto' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 1 as days
union all 
select 'Auto' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Auto' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 2 as days
union all 
select 'Auto' as linehaul_channel, 'delivering' as status, 3097 as status_int, 15 as days



union all 
select 'Aero' as linehaul_channel, 'clientPaymentSent' as status, 2010 as status_int, 4 as days
union all 
select 'Aero' as linehaul_channel, 'advancePaymentRequested' as status, 2050 as status_int, 4 as days
union all 
select 'Aero' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 2060 as status_int, 0 as days
union all 
select 'Aero' as linehaul_channel, 'PSI' as status, 2070 as status_int, 6 as days
union all 
select 'Aero' as linehaul_channel, 'remainingPaymentRequested' as status, 2080 as status_int, 4 as days
union all 
select 'Aero' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 2 as days
union all 
select 'Aero' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 7 as days
union all 
select 'Aero' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 6 as days
union all 
select 'Aero' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 2 as days
union all 
select 'Aero' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 1 as days
union all 
select 'Aero' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Aero' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 1 as days
union all 
select 'Aero' as linehaul_channel, 'delivering' as status, 3097 as status_int, 5 as days

union all 
select 'Sea' as linehaul_channel, 'clientPaymentSent' as status, 2010 as status_int, 4 as days
union all 
select 'Sea' as linehaul_channel, 'advancePaymentRequested' as status, 2050 as status_int, 4 as days
union all 
select 'Sea' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 2060 as status_int, 0 as days
union all 
select 'Sea' as linehaul_channel, 'PSI' as status, 2070 as status_int, 6 as days
union all 
select 'Sea' as linehaul_channel, 'remainingPaymentRequested' as status, 2080 as status_int, 4 as days
union all 
select 'Sea' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 5 as days
union all 
select 'Sea' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 5 as days
union all 
select 'Sea' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 10 as days
union all 
select 'Sea' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 21 as days
union all 
select 'Sea' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 3 as days
union all 
select 'Sea' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Sea' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 10 as days
union all 
select 'Sea' as linehaul_channel, 'delivering' as status, 3097 as status_int, 15 as days

union all 
select 'Rail' as linehaul_channel, 'clientPaymentSent' as status, 2010 as status_int, 4 as days
union all 
select 'Rail' as linehaul_channel, 'advancePaymentRequested' as status, 2050 as status_int, 4 as days
union all 
select 'Rail' as linehaul_channel, 'manufacturingAndQcInProgress' as status, 2060 as status_int, 0 as days
union all 
select 'Rail' as linehaul_channel, 'PSI' as status, 2070 as status_int, 6 as days
union all 
select 'Rail' as linehaul_channel, 'remainingPaymentRequested' as status, 2080 as status_int, 4 as days
union all 
select 'Rail' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 2 as days
union all 
select 'Rail' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 7 as days
union all 
select 'Rail' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 13 as days
union all 
select 'Rail' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 23 as days
union all 
select 'Rail' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 3 as days
union all 
select 'Rail' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Rail' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 1 as days
union all 
select 'Rail' as linehaul_channel, 'delivering' as status, 3097 as status_int, 5 as days
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

boxes_order as (
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

boxes_pickup as (
select 
merchant_order_id,
boxes.operationalProductId as product_id,
boxes.l as length,
boxes.w as width,
boxes.h as hight,
boxes.weight,
boxes.qty,
boxes.qtyPerBox as qty_per_box,
boxes.l*boxes.w*boxes.h/1000000 as measures
from
(
select
    explode(boxes) as boxes,
        _id as pickup_id,
        friendlyId as pickup_friendly_id,
        orderId as order_id,
        merchOrdId as merchant_order_id
    from {{ ref('scd2_pick_up_orders_snapshot') }}
    where dbt_valid_to is null
    )
    ),

boxes as (
select distinct * from boxes_pickup
union all 
select distinct * from boxes_order
where merchant_order_id||product_id not in (
    select distinct merchant_order_id||product_id from boxes_pickup
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
        
        case when s.sub_status = 'client2BrokerPaymentSent' then 'clientPaymentSent'
        when s.sub_status = 'pickupRequestSentToLogisticians' then 'pickupRequestSentToLogisticians'
        when s.sub_status = 'pickedUpByLogisticians' then 'pickedUpByLogisticians'
        when s.sub_status = 'arrivedAtLogisticsWarehouse' then 'arrivedAtLogisticsWarehouse'
        when s.sub_status = 'departedFromLogisticsWarehouse' then 'departedFromLogisticsWarehouse' 
        when status = 'shipping' then s.sub_status end as status,

        cast(id as int) as status_int
        from
    (
        select distinct
            order_id,
            date(event_ts_msk) as date_status,
            status,
            sub_status
        from {{ ref('fact_order_statuses') }}
        where (
         status in (
            'manufacturing'
            )
        and sub_status in (
            'client2BrokerPaymentSent',
            'joomSIAPaymentReceived'
            )
        )
        or status = 'shipping'
        order by status
    ) s
    left join b2b_mart.key_order_substatus k on s.sub_status = k.sub_status
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
        
        case when status = 'advancePaymentRequested' then 2010
        when status = 'manufacturingAndQcInProgress' then 2050
        when status = 'remainingPaymentRequested' then 2080 end as status_int
        from
    (
    select payload.id as merchant_order_id,
            case when payload.status = 'advancePaymentAcquired' then 'manufacturingAndQcInProgress'
                else  payload.status end as status,
            min(TIMESTAMP(millis_to_ts_msk(payload.updatedTime))) as day
            from {{ source('b2b_mart', 'operational_events') }}
            WHERE type  ='merchantOrderChanged'
            and payload.status in ('advancePaymentRequested', 'advancePaymentAcquired', 
                'manufacturingAndQcInProgress', 'remainingPaymentRequested')
            group by payload.id,
            case when payload.status = 'advancePaymentAcquired' then 'manufacturingAndQcInProgress'
                else  payload.status end
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
    case when status = 'readyForPsi' then 2070
        when status = 'pickupRequested' then 3010
        when status = 'pickupCompleted' then 3020 end as status_int
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
    case when status = 'WaitingForConfirmation' then 3010
        when status = 'Requested' then 3010
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
        3020 as status_int,
        planned_date as date_status,
        pickup_id
    from pickups
    union all 
    select 
        'pickupCompleted' as status,
        3020 as status_int,
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
        2070 as status_int,
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
row_number() over (partition by order_id, merchant_order_id, product_id, pickup_id order by status_int desc, priority desc, date_status desc) as rn
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

select
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
    day_diff,
    max(case when rn = 1 then status end) over (partition by order_id) as status_order,
    max(case when rn = 1 then status_int end) over (partition by order_id) as status_int_order,
    max(case when rn = 1 then date_status end) over (partition by order_id) as date_status_order,
    max(case when rn = 1 then day_diff end) over (partition by order_id) as day_diff_order,
    max(case when rn = 1 then date_add(date_status, int(day_diff)) end) over (partition by order_id) as predicted_date_order,
    max(case when rn = 1 then current_status_days end) over (partition by order_id) as current_status_days,
    max(case when rn = 1 then current_status_declared_days end) over (partition by order_id) as current_status_declared_days,
    date('{{ var("start_date_ymd") }}') as partition_date_msk

from
(
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
    max(total_time_spent + current_status) over 
        (partition by order_id, merchant_order_id, product_id, pickup_id, length,
        width, hight, weight, qty_per_box) as current_status_days,
    sum(case when past then days end) over 
        (partition by order_id, merchant_order_id, product_id, pickup_id, length,
        width, hight, weight, qty_per_box) as current_status_declared_days,
    sum(case when future then days end) over 
         (partition by order_id, merchant_order_id, product_id, pickup_id, length,
        width, hight, weight, qty_per_box) as day_diff,
    rank() over (partition by order_id order by status_int) as rn
from 
(
select distinct
    all.*,
    datediff(date_status, min_manufacturing_time) as total_time_spent,
    case when all.status_int = days.status_int then greatest(datediff(current_date, date_status), days) else 0 end as current_status,
    case when all.status_int = days.status_int then greatest(datediff(current_date, date_status), days)
        when all.status_int = 2050 and all.status_int = days.status_int then greatest(datediff(current_date, date_status), manufacturing_days)
        when days.status_int = 2050 then manufacturing_days
    else days end as days,
    all.status_int <= days.status_int and days.status_int < 3030 as future,
    all.status_int >= days.status_int as past
from all left join days on all.channel_type = days.linehaul_channel
)
)
