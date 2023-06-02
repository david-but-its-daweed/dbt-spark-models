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
  SELECT DISTINCT user_id as uid
  FROM {{ ref('dim_user') }}
  WHERE not fake or fake is null
),

user_interaction as 
(select 
    interaction_id, 
    user_id, 
    interaction_create_date, 
    date(interaction_create_date) as partition_date_msk,
    date(interaction_create_date) - WEEKDAY( date(interaction_create_date)) AS created_week,
    case when first_interaction_type then 1 else 0 end as first_interaction,
    utm_campaign,
    utm_source,
    utm_medium,
    source, 
    type,
    campaign,
    country,
    grade,
    validation_status,
    reject_reason,
    user_created_time,
    validated_date,
    repeated_order
    from (
        select 
            interaction_id, 
            user_id, 
            case when retention then 1 else 0 end as repeated_order,
            last_interaction_type,
            first_interaction_type,
            interaction_create_date,
            country,
            grade,
            validation_status,
            reject_reason,
            user_created_time,
            validated_date,
            max(case when last_interaction_type then utm_campaign end) over (partition by user_id) as utm_campaign,
            max(case when last_interaction_type then utm_source end) over (partition by user_id) as utm_source,
            max(case when last_interaction_type then utm_medium end) over (partition by user_id) as utm_medium,
            max(case when last_interaction_type then source end) over (partition by user_id) as source, 
            max(case when last_interaction_type then type end) over (partition by user_id) as type,
            max(case when last_interaction_type then campaign end) over (partition by user_id) as campaign
        from {{ ref('fact_attribution_interaction') }} m
        join not_jp_users n on n.uid = m.user_id
        
    )
)
,

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
    select distinct 
            interaction_id, 
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
    SELECT 
    order_id,
    gmv_initial as final_gmv,
    gmv_initial as client_converted_gmv
    from {{ ref('gmv_by_sources') }}
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
        interaction_create_date,
        partition_date_msk,
        created_week,
        FIRST_VALUE(utm_campaign) over (partition by user_id order by order_id is null, partition_date_msk) as utm_campaign,
        FIRST_VALUE(utm_source) over (partition by user_id order by order_id is null, partition_date_msk) as utm_source,
        FIRST_VALUE(utm_medium) over (partition by user_id order by order_id is null, partition_date_msk) as utm_medium,
        FIRST_VALUE(source) over (partition by user_id order by order_id is null, partition_date_msk) as source,
        FIRST_VALUE(type) over (partition by user_id order by order_id is null, partition_date_msk) as type,
        FIRST_VALUE(campaign) over (partition by user_id order by order_id is null, partition_date_msk) as campaign,
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
        rank(interaction_id) over (partition by user_id, interaction_min_time is not null order by interaction_min_time) as sucessfull_interaction_number,
        rank(interaction_id) over (partition by user_id order by 
                                   case when partition_date_msk <= order_min_time or order_min_time is null then 0 else 1 end) as sucessfull_order_number
        from
            (
            select 
                interaction_id,
                partition_date_msk,
                interaction_create_date,
                created_week,
                utm_campaign,
                utm_source,
                utm_medium,
                source, 
                type,
                campaign,
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
                interaction_create_date,
                partition_date_msk,
                created_week,
                utm_campaign,
                utm_source,
                utm_medium,
                source, 
                type,
                campaign,
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
                user_created_time as created_date,
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
            left join order_interaction oi on oi.interaction_id = in.interaction_id
            left join gmv on oi.order_id = gmv.order_id
            left join statuses s on current_status = s.status
            left join sub_statuses ss on current_status = ss.status and current_substatus = ss.sub_status
            where in.user_id != '000000000000000000000000'
            group by in.interaction_id,
            interaction_create_date,
                partition_date_msk,
                created_week,
                utm_campaign,
                utm_source,
                utm_medium,
                source, 
                type,
                campaign,
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
