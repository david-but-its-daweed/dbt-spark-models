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
  WHERE is_joompro_employee = TRUE
),

tags as
(
select distinct explode(tags) as tag, 
    _id as request_id 
    from  {{ source('mongo', 'b2b_core_popup_requests_daily_snapshot') }}
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
    campaign,
    website_form,
    repeated_order
    from (
        select 
            _id as interaction_id, 
            uid as user_id, 
            max(case when tag = 'repeated_order' then 1 else 0 end) as repeated_order,
            min(from_unixtime(ctms/1000 + 10800)) as interaction_create_date,
            max(map_from_entries(utmLabels)["utm_campaign"]) as utm_campaign,
            max(map_from_entries(utmLabels)["utm_source"]) as utm_source,
            max(map_from_entries(utmLabels)["utm_medium"]) as utm_medium,
            max(source) as source, 
            max(type) as type,
            max(campaign) as campaign,
            max(websiteForm) as website_form
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} m
        left join not_jp_users n on n.user_id = m.uid
        left join tags t on t.request_id = m.popupRequestID
        where n.user_id is null
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

users_hist as (
  select
    user_id,
    min(partition_date_msk) as validated_date
    from {{ ref('fact_user_change') }}
    group by user_id
),

users as (
select 
    du.user_id, 
    vs.status as validation_status, 
    rr.reason as reject_reason,
    date(created_ts_msk) as created_date,
    case when vs.status = 'validated' then validated_date end as validated_date
    from {{ ref('dim_user') }} du
    left join validation_status vs on du.validation_status = vs.id
    left join reject_reasons rr on du.reject_reason = rr.id
    left join users_hist uh on uh.user_id = du.user_id
    where next_effective_ts_msk is null
),

orders as (
    select distinct
        request_id,
        o.order_id, 
        friendly_id,
        s.status,
        s.sub_status,
        current_status,
        current_substatus,
        min_date
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
        sub_status,
        current_status,
        current_substatus,
        min(date(event_ts_msk)) as min_date
    from
        (
            select 
            order_id,
            status,
            sub_status,
            event_ts_msk,
            FIRST_VALUE(status) over (partition by order_id order by event_ts_msk desc) as current_status,
            FIRST_VALUE(sub_status) over (partition by order_id order by event_ts_msk desc) as current_substatus
            from {{ ref('fact_order_change') }}
        )
        group by order_id,
        status,
        sub_status,
        current_status,
        current_substatus
    ) s on o.order_id = s.order_id
),

order_interaction as (   
    select distinct interaction_id, 
            request_id,
            order_id,
            friendly_id,
            status,
            sub_status,
            current_status,
            current_substatus,
            min_date
        from
        (select distinct
            _id as interaction_id, 
            request_id,
            order_id,
            friendly_id,
            status,
            sub_status,
            current_status,
            current_substatus,
            min_date
        from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} i
        left join orders o on i.popupRequestId = o.request_id
    )
),

gmv as
(
SELECT order_id, final_gmv, client_converted_gmv
FROM
(
    SELECT  
        event_ts_msk,
        payload.orderId as order_id,
        payload.gmv.clientConvertedGMV AS client_converted_gmv,
        payload.gmv.finalGMV AS final_gmv,
        payload.gmv.finalGrossProfit AS final_gross_profit,
        payload.gmv.initialGrossProfit AS initial_gross_profit,
        row_number() over(partition by payload.orderId order by event_ts_msk desc) as rn
    FROM {{ source('b2b_mart', 'operational_events') }} oe
   )
where rn = 1
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
)

select
interaction_id,
    partition_date_msk,
    created_week,
    FIRST_VALUE(utm_campaign) over (partition by user_id order by order_id is null, partition_date_msk) as utm_campaign,
    FIRST_VALUE(utm_source) over (partition by user_id order by order_id is null, partition_date_msk) as utm_source,
    FIRST_VALUE(utm_medium) over (partition by user_id order by order_id is null, partition_date_msk) as utm_medium,
    FIRST_VALUE(source) over (partition by user_id order by order_id is null, partition_date_msk) as source,
    FIRST_VALUE(type) over (partition by user_id order by order_id is null, partition_date_msk) as type,
    FIRST_VALUE(campaign) over (partition by user_id order by order_id is null, partition_date_msk) as campaign,
    FIRST_VALUE(website_form) over (partition by user_id order by order_id is null, partition_date_msk) as website_form,
    repeated_order,
    user_id,
    validation_status, 
    reject_reason,
    first_interaction,
    request_id,
        order_id,
        friendly_id,
        current_status,
        current_substatus,
        final_gmv, client_converted_gmv,
        created_date,
        validated_date,
        min_status_new_ts_msk,
        min_status_selling_ts_msk,
        min_status_manufacturing_ts_msk,
        min_status_shipping_ts_msk,
        min_status_cancelled_ts_msk,
        min_status_closed_ts_msk,
        min_status_claim_ts_msk,
        min_price_estimation_ts_msk,
        min_negotiation_ts_msk,
        min_final_pricing_ts_msk,
        min_signing_and_payment_ts_msk,
        rn,
        interaction_min_time,
        source as current_source,
        utm_campaign as current_utm_campaign,
        utm_source as current_utm_source,
        utm_medium as current_utm_medium,
        type as current_type,
        campaign as current_campaign,
        website_form as current_website_form,
        rank(interaction_id) over (partition by user_id, interaction_min_time is not null order by interaction_min_time) as sucessfull_interaction_number,
        rank(interaction_id) over (partition by user_id order by 
                                   case when partition_date_msk <= order_min_time or order_min_time is null then 0 else 1 end) as sucessfull_order_number
        from
(
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
    website_form,
    repeated_order,
    user_id,
    validation_status, 
    reject_reason,
    first_interaction,
    request_id,
        order_id,
        friendly_id,
        current_status,
        current_substatus,
        final_gmv, client_converted_gmv,
        created_date,
        validated_date,
        min_status_new_ts_msk,
        min_status_selling_ts_msk,
        min_status_manufacturing_ts_msk,
        min_status_shipping_ts_msk,
        min_status_cancelled_ts_msk,
        min_status_closed_ts_msk,
        min_status_claim_ts_msk,
        min_price_estimation_ts_msk,
        min_negotiation_ts_msk,
        min_final_pricing_ts_msk,
        min_signing_and_payment_ts_msk,
        row_number() over (partition by interaction_id order by status_priority desc, substatus_priority desc, order_id) as rn,
        min(min_status_manufacturing_ts_msk) over (partition by user_id, interaction_id) as interaction_min_time,
        min(min_status_manufacturing_ts_msk) over (partition by user_id) as order_min_time
    from
    (select 
        in.interaction_id,
        partition_date_msk,
        created_week,
        utm_campaign,
        utm_source,
        utm_medium,
        source, 
        type,
        campaign,
        website_form,
        repeated_order,
        in.user_id,
        validation_status, 
        reject_reason,
        first_interaction,
        request_id,
        oi.order_id,
        friendly_id,
        current_status,
        current_substatus,
        final_gmv, client_converted_gmv,
        coalesce(s.priority, -1) as status_priority,
        coalesce(ss.priority, -1) as substatus_priority,
        created_date,
        validated_date,
        MIN(IF(oi.status = 'new', min_date, NULL)) AS min_status_new_ts_msk,
        MIN(IF(oi.status = 'selling', min_date, NULL)) AS min_status_selling_ts_msk,
        MIN(IF(oi.status = 'manufacturing', min_date, NULL)) AS min_status_manufacturing_ts_msk,
        MIN(IF(oi.status = 'shipping', min_date, NULL)) AS min_status_shipping_ts_msk,
        MIN(IF(oi.status = 'cancelled' and oi.current_status = 'cancelled', min_date, NULL)) AS min_status_cancelled_ts_msk,
        MIN(IF(oi.status = 'closed', min_date, NULL)) AS min_status_closed_ts_msk,
        MIN(IF(oi.status = 'claim', min_date, NULL)) AS min_status_claim_ts_msk,
        MIN(IF(oi.sub_status = 'priceEstimation', min_date, NULL)) AS min_price_estimation_ts_msk,
        MIN(IF(oi.sub_status = 'negotiation', min_date, NULL)) AS min_negotiation_ts_msk,
        MIN(IF(oi.sub_status = 'finalPricing', min_date, NULL)) AS min_final_pricing_ts_msk,
        MIN(IF(oi.sub_status = 'signingAndPayment', min_date, NULL)) AS min_signing_and_payment_ts_msk
    from user_interaction in 
    left join users u on in.user_id = u.user_id
    left join order_interaction oi on oi.interaction_id = in.interaction_id
    left join gmv on oi.order_id = gmv.order_id
    left join statuses s on current_status = s.status
    left join sub_statuses ss on current_status = ss.status and current_substatus = ss.sub_status
    where in.user_id != '000000000000000000000000'
    group by in.interaction_id,
        partition_date_msk,
        created_week,
        utm_campaign,
        utm_source,
        utm_medium,
        source, 
        type,
        campaign,
        website_form,
        repeated_order,
        in.user_id,
        validation_status, 
        reject_reason,
        first_interaction,
        request_id,
        oi.order_id,
        friendly_id,
        current_status,
        current_substatus,
        final_gmv, client_converted_gmv,
        coalesce(s.priority, -1),
        coalesce(ss.priority, -1),
        created_date,
        validated_date
    )
)
