{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
not_jp_users AS (
  SELECT DISTINCT u.user_id
  FROM {{ ref('fact_user_request') }} f
  LEFT JOIN {{ ref('fact_order') }} u ON f.user_id = u.user_id
  WHERE is_joompro_employee != TRUE or is_joompro_employee IS NULL
),

source as (
select 
    first_value(source) over (partition by uid order by ctms) as source,
    first_value(type) over (partition by uid order by ctms) as type,
    first_value(campaign) over (partition by uid order by ctms) as campaign,
    uid as user_id
    from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }}
),

user_interaction as 
(select 
     interaction_id, 
    user_id, 
    interaction_create_date, 
    date(interaction_create_date) as partition_date_msk,
    date(interaction_create_date) - WEEKDAY( date(interaction_create_date)) AS created_week,
    case when row_number() over(partition by user_id order by interaction_create_date) = 1 then 1 else 0 end as first_interaction,
    utm_campaign,
    utm_source,
    utm_medium,
    source, 
    type,
    campaign
    from (
        select 
            _id as interaction_id, 
            uid as user_id, 
            min(from_unixtime(ctms/1000 + 10800)) as interaction_create_date,
            max(map_from_entries(utmLabels)["utm_campaign"]) as utm_campaign,
            max(map_from_entries(utmLabels)["utm_source"]) as utm_source,
            max(map_from_entries(utmLabels)["utm_medium"]) as utm_medium,
            max(s.source) as source, 
            max(s.type) as type,
            max(s.campaign) as campaign
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} m
        inner join not_jp_users n on n.user_id = m.uid
        left join source as s on m.uid = s.user_id
        group by _id, uid
    )
)
,

validation_status as (
    SELECT
    10 AS id,
    'initial' AS status
    UNION ALL
    SELECT
        20 AS id,
        'needsValidation' AS status
    UNION ALL
    SELECT
        25 AS id,
        'onHold' AS status
    UNION ALL
    SELECT
        30 AS id,
        'validated' AS status
    UNION ALL
    SELECT
        40 AS id,
    'rejected' AS status
),

reject_reasons as (
    SELECT
    1010 AS id,
    'noFeedback' AS reason
UNION ALL
SELECT
    1020 AS id,
    'unsuitableDeliveryTerms' AS reason
UNION ALL
SELECT
    1030 AS id,
    'unsuitableFinancialTerms' AS reason
UNION ALL
SELECT
    1040 AS id,
    'unsuitableWarehouseTerms' AS reason
UNION ALL
SELECT
    1050 AS id,
    'unsuitableVolume' AS reason
UNION ALL
SELECT
    1060 AS id,
    'needsTimeForDecision' AS reason
UNION ALL
SELECT
    1070 AS id,
    'didNotSendRequest' AS reason
UNION ALL
SELECT
    1080 AS id,
    'deadRequest' AS reason
UNION ALL
SELECT
    1090 AS id,
    'uncommunicativeClient' AS reason
UNION ALL
SELECT
    1100 AS id,
    'noLegalEntity' AS reason
UNION ALL
SELECT
    1110 AS id,
    'other' AS reason
),

users as (
select 
    user_id, 
    vs.status as validation_status, 
    rr.reason as reject_reason
    from {{ ref('dim_user') }} du
    left join validation_status vs on du.validation_status = vs.id
    left join reject_reasons rr on du.reject_reason = rr.id
    where next_effective_ts_msk is null
),

statuses as (
    select 
        'new' as status, 0 as priority
    union all 
    select 
        'selling' as status, 2 as priority
    union all 
    select 
        'cancelled' as status, 1 as priority
    union all 
    select 
        'manufacturing' as status, 3 as priority
    union all 
    select 
        'shipping' as status, 4 as priority
    union all 
    select 
        'claim' as status, 5 as priority
    union all 
    select 
        'closed' as status, 6 as priority
),

sub_statuses as (
    select
        'selling' as status, 'new' as sub_status, 0 as priority
    union all 
     select
        'selling' as status, 'priceEstimation' as sub_status, 1 as priority
    union all 
     select
        'selling' as status, 'negotiation' as sub_status, 2 as priority
    union all 
     select
        'selling' as status, 'finalPricing' as sub_status, 3 as priority
    union all 
     select
        'selling' as status, 'signingAndPayment' as sub_status, 4 as priority
    union all 
     select
        'shipping' as status, 'new' as sub_status, 0 as priority
    union all 
     select
        'selling' as status, 'pickupRequestSentToLogisticians' as sub_status, 1 as priority
    union all 
     select
        'selling' as status, 'pickedUpByLogisticians' as sub_status, 2 as priority
    union all
    select
        'selling' as status, 'arrivedAtLogisticsWarehouse' as sub_status, 3 as priority
    union all
    select
        'selling' as status, 'departedFromLogisticsWarehouse' as sub_status, 4 as priority
    union all
    select
        'selling' as status, 'arrivedAtStateBorder' as sub_status, 5 as priority
    union all
    select
        'selling' as status, 'departedFromStateBorder' as sub_status, 6 as priority
    union all
    select
        'selling' as status, 'arrivedAtCustoms' as sub_status, 7 as priority
    union all
    select
        'selling' as status, 'customsDeclarationFiled' as sub_status, 8 as priority
    union all
    select
        'selling' as status, 'customsDeclarationReleased' as sub_status, 9 as priority
    union all
    select
        'selling' as status, 'delivered' as sub_status, 10 as priority
),

orders as (
    select distinct
        request_id,
        o.order_id, 
        friendly_id,
        s.status,
        s.sub_status,
        coalesce(statuses.priority, -1) as status_priority,
        coalesce(sub_statuses.priority, -1) as sub_status_priority
    from
    (
    select distinct 
        order_id, 
        friendly_id,
        request_id
        from {{ ref('fact_order') }}
        where next_effective_ts_msk is null) o
    left join 
    (   
         select 
        order_id,
        status,
        sub_status
    from
        (
            select 
            order_id,
            status,
            sub_status,
            row_number() over (partition by order_id order by event_ts_msk desc) as rn
            from {{ ref('fact_order_change') }}
        )
        where rn = 1
    ) s on o.order_id = s.order_id
    left join statuses on s.status = statuses.status
    left join sub_statuses on s.sub_status = sub_statuses.sub_status and s.status = sub_statuses.status
),

order_interaction as (   
    select interaction_id, 
            request_id,
            order_id,
            friendly_id,
            status,
            sub_status,
            status_priority,
            sub_status_priority
            from
    (
    select interaction_id, 
            request_id,
            order_id,
            friendly_id,
            status,
            sub_status,
            status_priority,
            sub_status_priority,
            row_number() over (partition by interaction_id order by status_priority desc, sub_status_priority desc, sub_status, friendly_id) as rn
        from
        (select distinct
            _id as interaction_id, 
            request_id,
            order_id,
            friendly_id,
            status,
            sub_status,
            status_priority,
            sub_status_priority
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} i
        left join orders o on i.popupRequestId = o.request_id
    )
    )
    where rn = 1
),

gmv as
(
SELECT interaction_id, final_gmv, client_converted_gmv
FROM
(
    SELECT  
        event_ts_msk,
        interaction_id,
        payload.gmv.clientConvertedGMV AS client_converted_gmv,
        payload.gmv.finalGMV AS final_gmv,
        payload.gmv.finalGrossProfit AS final_gross_profit,
        payload.gmv.initialGrossProfit AS initial_gross_profit,
        row_number() over(partition by interaction_id order by event_ts_msk desc) as rn
    FROM {{ source('b2b_mart', 'operational_events') }} oe
    right join
    (select distinct
            _id as interaction_id, 
            request_id,
            order_id
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} i
        left join orders o on i.popupRequestId = o.request_id
    ) in on oe.payload.orderId = in.order_id
    WHERE `type`  ='orderChangedByAdmin'
    )
where rn = 1
)


select 
    interaction_id,
    partition_date_msk,
    created_week,
    utm_campaign,
    utm_source,
    utm_medium,
    source, 
    type,
    campaign,
    user_id,
    validation_status, 
    reject_reason,
    first_interaction,
    request_id,
    order_id,
    friendly_id,
    status,
    sub_status,
    final_gmv, client_converted_gmv,
    funnel_field,
    int_funnel_field,
    cast(int_funnel_field as string)||"_"||funnel_field as sorted_funnel_field,
    order_successful,
    row_number() over(partition by user_id, order_successful order by partition_date_msk) as order_number
    from
    (select distinct
        in.interaction_id,
        partition_date_msk,
        created_week,
        utm_campaign,
        utm_source,
        utm_medium,
        source, 
        type,
        campaign,
        in.user_id,
        validation_status, 
        reject_reason,
        first_interaction,
        request_id,
        order_id,
        friendly_id,
        status,
        sub_status,
        final_gmv, client_converted_gmv,
        case when status = 'cancelled' then 'cancelled'
            when status = 'claim' then 'claim'
            when status = 'closed' then 'closed'
            when status = 'shipping' then 'shipping'
            when status = 'manufacturing' then 'manufacturing'
            when status = 'selling' and sub_status = 'signingAndPayment' then 'signing'
            when status = 'selling' and sub_status = 'finalPricing' then 'final Pricing'
            when status = 'selling' and sub_status = 'negotiation' then 'negotiation'
            when status = 'selling' and sub_status = 'priceEstimation' then 'price Estimation'
            when status = 'selling' and sub_status = 'new' then 'new in selling'
            when validation_status in ('validated', 'rejected') then validation_status 
            else 'Not validated' end as funnel_field,
        case when status = 'cancelled' then 12
            when status = 'claim' then 11
            when status = 'closed' then 10
            when status = 'shipping' then 9
            when status = 'manufacturing' then 8
            when status = 'selling' and sub_status = 'signingAndPayment' then 7
            when status = 'selling' and sub_status = 'finalPricing' then 6
            when status = 'selling' and sub_status = 'negotiation' then 5
            when status = 'selling' and sub_status = 'priceEstimation' then 4
            when status = 'selling' and sub_status = 'new' then 3
            when validation_status = 'validated' then 2
            when validation_status = 'rejected' then 1
            else 0 end as int_funnel_field,
        case when status in ('claim', 'closed', 'shipping', 'manufacturing') then 1 else 0 end as order_successful
    from user_interaction in 
    left join users u on in.user_id = u.user_id
    left join order_interaction oi on oi.interaction_id = in.interaction_id
    left join gmv on in.interaction_id = gmv.interaction_id
    where in.user_id != '000000000000000000000000'
    )
