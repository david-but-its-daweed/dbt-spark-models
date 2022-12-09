{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with user_interaction as 
(select 
     interaction_id, 
    user_id, 
    interaction_create_date, 
    date(interaction_create_date) as partition_date_msk,
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
            max(source) as source, 
            max(type) as type,
            max(campaign) as campaign
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }}
        group by _id, uid
    )
)
,

fact_orders as (
    select distinct 
        order_id, 
        friendly_id,
        request_id
        from {{ ref('fact_order') }}
        where next_effective_ts_msk is null
        ),

order_interaction as
(select distinct
            _id as interaction_id, 
            request_id,
            order_id,
            friendly_id
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} i
        left join fact_orders o on i.popupRequestId = o.request_id
    ),
    
admin AS (
    SELECT
        admin_id,
        email
    FROM {{ ref('dim_user_admin') }}
),

orders AS
(
    SELECT
        o.order_id,
        o.status,
        o.sub_status,
        o.event_ts_msk,
        FIRST_VALUE(ao.email) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email
    FROM {{ ref('fact_order_change') }} AS o
    LEFT JOIN admin AS ao ON o.owner_moderator_id = ao.admin_id
    UNION ALL 
    SELECT
        order_id,
        'validation' AS status,
        validation_status AS sub_status,
        event_ts_msk,
        FIRST_VALUE(ao.email) OVER (PARTITION BY fo.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email
    FROM {{ ref('fact_user_change') }} AS o
    LEFT JOIN admin AS ao ON o.owner_moderator_id = ao.admin_id
    INNER JOIN (
        select distinct user_id, order_id from {{ ref('fact_order')}} WHERE next_effective_ts_msk IS NULL
    ) AS fo ON fo.user_id = o.user_id
),

status_history AS (
    SELECT DISTINCT order_id, event_ts_msk, status, sub_status, lead_status, lead_sub_status, current_status, current_sub_status, owner_moderator_email
    FROM
    (
    SELECT
        o.order_id,
        o.status,
        o.sub_status,
        o.event_ts_msk,
        FIRST_VALUE(o.owner_moderator_email) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        LEAD(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status_ts,
        LEAD(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status,
        LEAD(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status
    FROM orders AS o
    )
    WHERE status != lead_status OR sub_status != lead_sub_status OR lead_status IS NULL
),


statuses as (
    select 
        'new' as status, '0' as priority
    union all 
    select 
        'selling' as status, '2' as priority
    union all 
    select 
        'cancelled' as status, '1' as priority
    union all 
    select 
        'manufacturing' as status, '3' as priority
    union all 
    select 
        'shipping' as status, '4' as priority
    union all 
    select 
        'claim' as status, '5' as priority
    union all 
    select 
        'closed' as status, '6' as priority
),

sub_statuses as (
    select
        'selling' as status, 'new' as sub_status, '00' as priority
    union all 
     select
        'selling' as status, 'priceEstimation' as sub_status, '01' as priority
    union all 
     select
        'selling' as status, 'negotiation' as sub_status, '02' as priority
    union all 
     select
        'selling' as status, 'finalPricing' as sub_status, '03' as priority
    union all 
     select
        'selling' as status, 'signingAndPayment' as sub_status, '04' as priority
    union all 
     select
        'shipping' as status, 'new' as sub_status, '00' as priority
    union all 
     select
        'selling' as status, 'pickupRequestSentToLogisticians' as sub_status, '01' as priority
    union all 
     select
        'selling' as status, 'pickedUpByLogisticians' as sub_status, '02' as priority
    union all
    select
        'selling' as status, 'arrivedAtLogisticsWarehouse' as sub_status, '03' as priority
    union all
    select
        'selling' as status, 'departedFromLogisticsWarehouse' as sub_status, '04' as priority
    union all
    select
        'selling' as status, 'arrivedAtStateBorder' as sub_status, '05' as priority
    union all
    select
        'selling' as status, 'departedFromStateBorder' as sub_status, '06' as priority
    union all
    select
        'selling' as status, 'arrivedAtCustoms' as sub_status, '07' as priority
    union all
    select
        'selling' as status, 'customsDeclarationFiled' as sub_status, '08' as priority
    union all
    select
        'selling' as status, 'customsDeclarationReleased' as sub_status, '09' as priority
    union all
    select
        'selling' as status, 'delivered' as sub_status, '10' as priority
)


SELECT DISTINCT
    ui.user_id, 
    ui.interaction_id, 
    oi.request_id,
    oi.order_id,
    oi.friendly_id,
    sh.status, 
    sh.sub_status, 
    sh.lead_status, 
    sh.lead_sub_status, 
    sh.current_status,
    sh.current_sub_status,
    sh.owner_moderator_email,
    ui.utm_campaign,
    ui.utm_source,
    ui.utm_medium,
    ui.source, 
    ui.type,
    ui.campaign,
    date(sh.event_ts_msk) as partition_date_msk,
    CONCAT(CASE WHEN s.priority is null then '7' else s.priority end, CONCAT('/', sh.status)) AS ordered_status,
    CONCAT(CASE WHEN ss.priority is null then '11' else ss.priority end, CONCAT('/', sh.sub_status)) AS ordered_sub_status,
    CONCAT(CONCAT(s.priority, CASE WHEN ss.priority is null then '11' else ss.priority end), CONCAT('/', CONCAT(sh.status,CONCAT('/', sh.sub_status)))) AS ordered_staus_sub_status
from 
user_interaction AS ui
LEFT JOIN order_interaction AS oi ON ui.interaction_id = oi.interaction_id
LEFT JOIN status_history AS sh ON sh.order_id = oi.order_id
LEFT JOIN statuses s ON sh.status = s.status
LEFT JOIN sub_statuses ss ON sh.status = ss.status AND sh.sub_status = ss.sub_status
where ui.user_id != '000000000000000000000000'
