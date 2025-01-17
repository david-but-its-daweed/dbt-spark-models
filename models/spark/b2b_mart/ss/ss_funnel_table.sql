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

WITH bounce AS (
    
    SELECT
        user['userId'] AS user_id
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE payload.pageUrl like '%https://joom.pro/pt-br%'
    GROUP BY 1
    HAVING SUM(IF(type IN ('sessionStart', 'bounceCheck', "productPreview", "searchOpen", "categoryClick", "orderPreview", "productGalleryScroll", "categoryOpen", "popupFormSubmit", "popupFormNext", "mainClick", "orderClick"), 1, 0)) > 0
),
samples as (
    select deal_id, 
    max(case when sample_type is not null then 0 else 1 end) as sample 
 from {{ ref('fact_customer_requests') }}
 group by 1
    ),
deals as (
    SELECT
        user_id,
        created_ts_msk - INTERVAL 3 hours AS deal_created_ts,
        CAST(created_ts_msk - INTERVAL 3 hours AS DATE) AS deal_created_date,
        CASE WHEN owner_email is not null then owner_ts - INTERVAL 3 hours end AS owner_ts_msk,
        CAST(CASE WHEN owner_email is not null then owner_ts - INTERVAL 3 hours end AS DATE) AS owner_date_msk
    FROM {{ ref('fact_deals') }} 
    join samples using(deal_id)
    WHERE next_effective_ts_msk is null
        and sample = 0 
        AND (self_service or ss_customer)
),

utm_labels as (
    SELECT DISTINCT
        first_value(labels.utm_source) over (partition by user_id order by event_ts_msk) as utm_source,
        first_value(labels.utm_medium) over (partition by user_id order by event_ts_msk) as utm_medium,
        first_value(labels.utm_campaign) over (partition by user_id order by event_ts_msk) as utm_campaign,
        user_id
    from
    (
    select
    map_from_arrays(transform(
            split(split_part(payload.pageUrl, "?", -1), '&'), 
            x -> case when split_part(x, '=', 1) != "gclid" then split_part(x, '=', 1) else "gclid" || uuid() end
        ),
        
        transform(
            split(split_part(payload.pageUrl, "?", -1), '&'), 
            x -> split_part(x, '=', 2)
        )
        ) as labels,
        user.userId as user_id, event_ts_msk
    from {{ source('b2b_mart', 'device_events') }}
    where type = 'sessionStart' and payload.pageUrl like "%utm%" and payload.pageUrl not like  '%https://joompro.ru/ru%'
    )
),

autorisation_date as (
     SELECT DISTINCT
        user['userId'] AS user_id,
        min(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE  then event_ts_msk end) as autorisation_ts
    FROM {{ source('b2b_mart', 'device_events') }}
    GROUP BY user['userId']
)


    SELECT DISTINCT
        user_id,
        max(case when type in ('sessionStart', 'bounceCheck')
                AND payload.pageUrl like '%https://joom.pro/pt-br%' 
                AND (event_ts_msk <= autorisation_ts OR autorisation_ts IS NULL)
            then event_ts_msk end - INTERVAL 3 hours) AS visit_ts,
        CAST(max(case when type in ('sessionStart', 'bounceCheck')
                AND payload.pageUrl like '%https://joom.pro/pt-br%'
                AND (event_ts_msk <= autorisation_ts OR autorisation_ts IS NULL)
            then event_ts_msk end - INTERVAL 3 hours) AS DATE) AS visit_date,
        max(case when type in ('sessionStart', 'bounceCheck') AND payload.pageUrl like '%https://joom.pro/pt-br%'
            AND (event_ts_msk <= autorisation_ts OR autorisation_ts IS NULL)
            then 1 else 0 end) AS visit,
         
        min(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE  then event_ts_msk end - INTERVAL 3 hours) as autorisation_ts,
        CAST(min(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE  then event_ts_msk end - INTERVAL 3 hours) AS DATE) as autorisation_date,
        max(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE  then 1 else 0 end) AS autorisation,
        
        min(case when type = 'selfServiceRegistrationFinished' then event_ts_msk end - INTERVAL 3 hours) as registration_ts,
        CAST(min(case when type = 'selfServiceRegistrationFinished' then event_ts_msk end - INTERVAL 3 hours) AS DATE) as registration_date,
        max(case when type = 'selfServiceRegistrationFinished' then 1 else 0 end) as registration,
    
        min(case when type = 'addToCart' then event_ts_msk end - INTERVAL 3 hours) as addToCart_ts,
        CAST(min(case when type = 'addToCart' then event_ts_msk end - INTERVAL 3 hours) AS DATE) as addToCart_date,
        max(case when type = 'addToCart' then 1 else 0 end) as addToCart,
    
        min(case when type = 'checkoutStartClick' then event_ts_msk end - INTERVAL 3 hours) as checkoutStartClick_ts,
        CAST(min(case when type = 'checkoutStartClick' then event_ts_msk end - INTERVAL 3 hours) AS DATE) as checkoutStartClick_date,
        max(case when type = 'checkoutStartClick' then 1 else 0 end) as checkoutStartClick,

        min(case when type = 'checkoutFinishClick' then event_ts_msk end - INTERVAL 3 hours) as checkoutFinishClick_ts,
        CAST(min(case when type = 'checkoutFinishClick' then event_ts_msk end - INTERVAL 3 hours) AS DATE) as checkoutFinishClick_date,
        max(case when type = 'checkoutFinishClick' then 1 else 0 end) as checkoutFinishClick,
    
        
        min(case when type = 'orderCreateClick' then event_ts_msk end - INTERVAL 3 hours) as order_create_ts,
        CAST(min(case when type = 'orderCreateClick' then event_ts_msk end - INTERVAL 3 hours) AS DATE) as order_create_date,
        max(case when type = 'orderCreateClick' then 1 else 0 end) as order_create,
        
        min(case when deal_created_ts is not null then deal_created_ts end) as deal_created_ts,
        CAST(min(case when deal_created_ts is not null then deal_created_ts end) AS DATE) as deal_created_date,
        max(case when deal_created_ts is not null then 1 else 0 end) as deal_created,
        
        min(case when owner_ts_msk is not null then owner_ts_msk end) as deal_assigned_ts,
        CAST(min(case when owner_ts_msk is not null then owner_ts_msk end) AS DATE) as deal_assigned_date,
        max(case when owner_ts_msk is not null then 1 else 0 end) as deal_assigned

     FROM {{ source('b2b_mart', 'device_events') }}
     INNER JOIN bounce
        ON bounce.user_id = user['userId']
     LEFT JOIN deals USING (user_id)
     LEFT JOIN autorisation_date USING (user_id)
        WHERE  partition_date >= '2024-04-06'
     GROUP BY user_id

