{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-07-10', '2023-09-16', '2023-09-15', '2023-10-18', '2023-11-16', '2023-11-29', '2023-11-30', '2023-12-01', '2023-12-02']
    }
) }}

with 
days as (

select * from {{ ref('sla_days') }}

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
        d.order_product_id,
        o.channel_type,
        o.partition_date_msk as order_created_time,
        o.min_manufacturing_time,
        mo.merchant_order_id,
        mo.merchant_order_friendly_id,
        manufacturing_days,
        b.product_id,
        pickup_id,
        pickup_friendly_id
    from {{ ref('dim_deal_products') }} d
    left join orders o on d.order_id = o.order_id
    left join merchant_orders mo on o.order_id = mo.order_id
    left join boxes b on b.merchant_order_id = mo.merchant_order_id
    left join pickups p on o.order_id = p.order_id and mo.merchant_order_id = p.merchant_order_id
    and p.product_id = b.product_id
    where o.min_manufacturing_time >= '2022-07-01' and b.product_id is not null
    ),
    

statuses as (
    select 
        d.*,
        date_status,
        status, 
        status_int,
        case when status = 'manufacturing' then manufacturing_days else days end as days
    from
    dict d
    left join (select order_id,
        merchant_order_id,
        order_product_id, 
        manufacturing_days,
        key as status,
        value as date_status
    from
    (select
       order_product_id,
       manufacturing_days,
       order_id, merchant_order_id,
       explode(
           map(
               'clientPayment',
               date(client_to_broker_payment_sent),
               'advancePayment',
               date(advance_payment_requested),
               'manufacturing',
               date(product_manufacturing),
               'psi',
               date(psi),
               'fixingPsi',
               date(psi_failed_time),
               'remainingPayment',
               date(remaining_payment_requested)
           )
        )
    from {{ ref('jp_sla_production') }}
    )
    where value is not null) using (order_product_id,
       manufacturing_days,
       order_id)
    left join days using (status)
    
    )
,

order_statuses as (
    select 
        d.*,
        date_status,
        status, 
        status_int,
        days
    from
    dict d
    left join 
    (select
        order_id,
        date(event_ts_msk) as date_status,
        sub_status as status
    from {{ ref('fact_order_statuses_change') }}
    where status = 'shipping'
    ) using (order_id)
    left join days using (status)
)

,

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
    cast(order_created_time as timestamp) as order_created_time,
    min_manufacturing_time,
    merchant_order_id,
    merchant_order_friendly_id,
    cast(manufacturing_days as int) as manufacturing_days,
    product_id,
    pickup_id,
    pickup_friendly_id,
    status,
    status_int,
    date_status
from
(
select * , 
row_number() over (partition by order_id, merchant_order_id, product_id, pickup_id order by status_int desc, date_status desc) as rn
from
(
select * from 
order_statuses 
union all 
select * from 
statuses

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
    when all.status_int = 2050 and all.status_int = days.status_int then greatest(datediff(current_date, date_status), cast(manufacturing_days as int))
    when days.status_int = 2050 then cast(manufacturing_days as int)
    else days end as days,
    all.status_int <= days.status_int and days.status_int < 3030 as future,
    all.status_int >= days.status_int as past
from all left join days on all.channel_type = days.linehaul_channel
)
where order_id is not null
)
