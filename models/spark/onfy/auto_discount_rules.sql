{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}
    
with price_history as 
(
    select 
        effective_ts,
        product_id,
        pzn,
        manufacturer_name,
        min(price) as min_price
    from {{source('onfy_mart', 'dim_product')}}
    where 1=1
        and store_state = 'DEFAULT'
        and is_current
        and coalesce(stock_quantity, 0) > 0
        and effective_ts >= current_date() - interval 4 days
    group by 
        effective_ts,
        product_id,
        pzn,
        manufacturer_name
),

price_change as 
(
    select
        effective_ts,
        product_id,
        pzn,
        if(lag(min_price) over (partition by product_id order by effective_ts) > min_price, 1, 0) as price_change
    from price_history
),

latest_price as 
(
    select 
        max(effective_ts) as max_effective_ts,
        product_id,
        pzn,
        max_by(price_change, effective_ts) as latest_price_decrease
    from price_change
    group by 
        product_id,
        pzn
),

manufacturers_products as 
(
    select distinct 
        product_id
    from price_history
    where 1=1
        and manufacturer_name = 'MEDICE Arzneimittel Pütter GmbH&Co.KG'
),

billiger_sessions as 
(
    select distinct
        device_events.payload.params.utm_source as utm_source,
        coalesce(if(split(device_events.payload.link, '/')[2] like '%?utm%', left(split(device_events.payload.link, '/')[2], 8), split(device_events.payload.link, '/')[2]), 
        device_events.payload.link) as pzn,
        device_id,
        event_ts_cet as session_ts_cet,
        event_id as event_id,
        if(event_ts_cet >= current_date() - interval 90 days, event_id, null) as window_event_id,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_session_ts_cet
    from 
        {{source('onfy_mart', 'device_events')}} as device_events
     where 1=1
        and device_events.partition_date_cet >= current_date() - interval 97 days
        and device_events.payload.params.utm_source like '%billiger%'
        and device_events.payload.link is not null
),

billiger_transactions as 
(
    select 
        order_id,
        device_id,
        transaction_date,
        sum(if(type = 'DISCOUNT' and transaction_date >= current_date() - interval 90 days, price, 0)) as promocode_discount,
        sum(if(transaction_date >= current_date() - interval 90 days, gmv_initial, 0)) as gmv_initial,
        sum(if(transaction_date >= current_date() - interval 90 days, gross_profit_initial, 0)) as gross_profit_initial
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
        and transaction_date >= current_date() - interval 91 days
    group by 
        order_id,
        transaction_date,
        device_id
),

billiger_data as 
(
    select
        utm_source,
        billiger_sessions.pzn,
        count(distinct billiger_sessions.event_id) as clicks,
        count(distinct billiger_transactions.order_id) as payments,
        if(
            count(distinct billiger_transactions.order_id) = 0 or 
            sum(gross_profit_initial + promocode_discount) / (count(distinct billiger_sessions.window_event_id) * 0.28 + sum(promocode_discount)) <= 0.7, 0, 
            count(distinct billiger_transactions.order_id) / count(distinct billiger_sessions.event_id)
        ) as cr,
        latest_price.latest_price_decrease,
        count(distinct billiger_sessions.event_id) * 0.28 as cost,
        sum(cast(promocode_discount as float)) as promocode_discount,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_initial) as gross_profit_initial,
        sum(gross_profit_initial + promocode_discount) / (count(distinct billiger_sessions.window_event_id) * 0.28 + sum(promocode_discount)) as roas

    from 
        billiger_sessions
    left join billiger_transactions
        on billiger_sessions.device_id = billiger_transactions.device_id
        and billiger_transactions.transaction_date between billiger_sessions.session_ts_cet and coalesce(billiger_sessions.next_session_ts_cet, current_timestamp())
        and to_unix_timestamp(billiger_transactions.transaction_date) - to_unix_timestamp(billiger_sessions.session_ts_cet) <= 86400
    left join latest_price
        on latest_price.pzn = billiger_sessions.pzn
    group by 
        utm_source,
        billiger_sessions.pzn,
        latest_price_decrease
),

idealo_sessions as 
(
    select distinct
        device_events.payload.params.utm_source as utm_source,
        coalesce(if(split(device_events.payload.link, '/')[2] like '%?utm%', left(split(device_events.payload.link, '/')[2], 8), split(device_events.payload.link, '/')[2]), 
        device_events.payload.link) as pzn,
        device_id,
        event_ts_cet as session_ts_cet,
        event_id as event_id,
        if(event_ts_cet >= current_date() - interval 90 days, event_id, null) as window_event_id,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_session_ts_cet
    from 
        {{source('onfy_mart', 'device_events')}} as device_events
     where 1=1
        and device_events.partition_date_cet >= current_date() - interval 97 days
        and device_events.payload.params.utm_source like '%idealo%'
        and device_events.payload.link is not null
),

idealo_transactions as 
(
    select 
        order_id,
        device_id,
        transaction_date,
        sum(if(type = 'DISCOUNT' and transaction_date >= current_date() - interval 90 days, price, 0)) as promocode_discount,
        sum(if(transaction_date >= current_date() - interval 90 days, gmv_initial, 0)) as gmv_initial,
        sum(if(transaction_date >= current_date() - interval 90 days, gross_profit_initial, 0)) as gross_profit_initial
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
        and transaction_date >= current_date() - interval 91 days
    group by 
        order_id,
        transaction_date,
        device_id
),

idealo_data as 
(
    select
        utm_source,
        idealo_sessions.pzn,
        count(distinct idealo_sessions.event_id) as clicks,
        count(distinct idealo_transactions.order_id) as payments,
        if(
            count(distinct idealo_transactions.order_id) = 0 or 
            sum(gross_profit_initial + promocode_discount) / (count(distinct idealo_sessions.window_event_id) * 0.44 + sum(promocode_discount)) <= 0.35, 0, 
            count(distinct idealo_transactions.order_id) / count(distinct idealo_sessions.event_id)
        ) as cr,
        latest_price.latest_price_decrease,
        count(distinct idealo_sessions.event_id) * 0.44 as cost,
        sum(cast(promocode_discount as float)) as promocode_discount,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_initial) as gross_profit_initial,
        sum(gross_profit_initial + promocode_discount) / (count(distinct idealo_sessions.window_event_id) * 0.44 + sum(promocode_discount)) as roas

    from 
        idealo_sessions
    left join idealo_transactions
        on idealo_sessions.device_id = idealo_transactions.device_id
        and idealo_transactions.transaction_date between idealo_sessions.session_ts_cet and coalesce(next_session_ts_cet, current_timestamp())
        and to_unix_timestamp(idealo_transactions.transaction_date) - to_unix_timestamp(idealo_sessions.session_ts_cet) <= 86400
    left join latest_price
        on latest_price.pzn = idealo_sessions.pzn
    group by 
        utm_source,
        idealo_sessions.pzn,
        latest_price_decrease
),

google_data as 
(    
    select
        if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2]) as landing_pzn,
        count(distinct order_id) as orders,
        sum(session_spend) as spend,
        count(sources.device_id) as sessions
    from {{source('onfy', 'sources')}} as sources
    join {{source('onfy', 'ads_dashboard')}} as ads_dashboard
        on sources.device_id = ads_dashboard.device_id
        and sources.source_dt = ads_dashboard.session_dt
    where 1=1
        and session_dt >= current_date() - interval 3 month
        and source = 'google'
        and campaign like 'shopping%'
        and if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2]) is not null
        and len(if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2])) = 8
    group by 
        if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2])
    having orders = 0
),

precalculation as 
(
    select 
        m.id as product_id, 
        null as store_id, 
        'google' as channel, 
        31 as weight, 
        'filter_out' as pessimization_type, 
        0.0 as discount_percent, 
        'blacklist' as source, 
        'Google Shopping low performing blacklist' as comment
    from google_data cr
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.landing_pzn
    where orders < 1
    union all
    select 
        m.id as product_id, 
        null as store_id, 
        'ohm' as channel, 
        31 as weight, 
        'filter_out' as pessimization_type, 
        0.0 as discount_percent, 
        'blacklist' as source, 
        'OHM Google Shopping low performing blacklist' as comment
    from google_data cr
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.landing_pzn
    where orders < 1
    union all
    select 
        m.id as product_id, 
        null as store_id, 
        'idealo' as channel, 
        31 as weight, 
        'use_max_price' as pessimization_type, 
        0.0 as discount_percent, 
        'low_performing' as source, 
        'Idealo low performing blacklist' as comment
    from idealo_data as cr
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.pzn
    where (cr.cr <= 0.001 and cr.latest_price_decrease <> 1)
    union all
    select 
        m.id as product_id, 
        null as store_id, 
        'billiger' as channel, 
        31 as weight, 
        'use_max_price' as pessimization_type, 
        0.0 as discount_percent, 
        'low_performing' as source, 
        'Billiger low performing blacklist' as comment
    from billiger_data cr 
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.pzn
    where (cr.cr <= 0.001 and cr.latest_price_decrease <> 1)
)

select *
from precalculation 
where 1=1
    and product_id not in (select distinct product_id from manufacturers_products)
    and product_id not in ('60756cdf800b4b3b8b54a52ccdd50235', '53347d26b34e4ae0a8ff0a31b54bde42', 'd7c6174b465446ae901be9b319e149cb', 
    'c1a168e8406d432395b1d9589b395bd9', 'a5d3845d14b74c29b872e9e98053d322', 'e909862fdb4040c1807ecab2e23fad3d', 'c84d77551f43442a8c8bd736531c07ac',
    '8ca566875f5b4d378c3846c9941142aa', '0d5f5865ec8d4e1f80862bfbfb415dd2', 'cde59bd370c944328fc2bae744f2bb55', '3f9456673493408d971c74b18ac5f09a', 
    '63af0cd529e344719b5d3b91bd5d57f1', 'e593c9081860457bb46377527c5ea525', '1c72f6f1b64046779fa0b16b7704af3f', 'e12aaaed79f74e178e06ac7b978b8f6c', 
    'f1bc1675494a424694343e96dd7b2090', '74f2229c2ede44cdbd4c4bd1f3549229', '0116837ee1b14d8d9435aabcdc9b1a5d', '5a1c1d6fa8c745408db445423950512b', 
    'd809bca800b741b98d71872dd7293b88', '6b0c6dd43ed845f2a84ca3735764cbdb', '58882f8286024c568a91d5fa2b880d5a', 'cdf9c651f87d443c87c1e935b6fa9a16', 
    '10d00ef2695b40d19c650fc6bfebfacd', 'b5172da866e343619b7226b3ac6b05b9', '7eee7cd75ff74a74943a940099515e0d', '565b96f34fab42cb99e1d31d02cbfc45', 
    '44f6be5e892e40a0aca9c5b2fb0abf30', '31be8074185d4b188115e0dcd3ddca1a', '08fc43ef0cb3441a8301d971a797afda', '38b663617bf54d0aa70dadcfbcbe4e4d', 
    'e4f5cf464fdc48b699d82669f462f5fd', '4621f268787e40b0aec4c6cc9e3aaf3c', '89da685b7f184813af5fef501f012549', 'd5f0a9af274345d087dc517a3faa42a7', 
    'c19f4c70de094ccba3698a79c3898d6b', 'f0906355beb54f7fbcaac3892613fe50', '7ed6f973fa0a4a9ca02926b6816de736', 'e76ec52a211b486f986751d3a660b823', 
    '66b14cbdfa634a359596c4cf47731803', '9ab6a8c6732244d0aa906840719783ba', 'e2d2cdf973da4519be5884b3b3d9e5ae', '5fdfdd37705849e1bc2bc281ff6999b9', 
    'cc986eca17564318abe6fbd7dce3f56a', 'a28879988d6d446abebddf8199a5db20', 'e821f5f9d2c242868805307697eadd1b', 'b77c6cc1e9da437ca0e9d23dec960ab4', 
    '51bb0a2b4b7d42519590ac390b6f50d3', 'd60a7b22047f42b1ad570d07f4cedc39', '269e332699024e729fc382f98de8a300', '23fd6faa65a34fb29e313543d25ea692', 
    '2ec2e758787e49ab949615a94a0ac497', '8a5b0fcd587b48f3a537d145dc809a51', '02dbca7fe54c4871aee877eb7985608b', 'db0b9fa929684deb8e33754c29516ef9', 
    '7478d8dd999e4c29a95cd07beb08204a', '0604f96b019841a49dd62a5c677ec3f6', '063fa7dbb3bf4bbbb04f5d1560031c0d', '03d8b5bfd3954ced9d56e04c038584b2', 
    'faa53a421692482794b3cdca1ff0d8e1', '8dce04bec6864c2c96481bf35e47e0ab', '7346253d08c74149a2ca68f0972c465c', 'fa18c57b26f046a79034e1c20c8c3c12', 
    'c6e114e101ff4de58ff542e10ef2cbb9', 'abe43bf2fe6e432ea2513b6eb16b6b8c', '99d94b28dfae47d48acfcd2deb10f57b', 'd866ac981e634a9aa34c2439edf4b627', 
    'd2e3a5ab7b654a438ae086766c780d2f', 'cab944499510468db6427babe6ed6cfa', 'c34ff002532048acb245b94c907ed0ad', 'b0c50c81756f4eaf949297990797f9ee', 
    'd181ebf82090420e8438cc31ef3d2aeb', 'ec22c75d2fb7453fa48bbd94bf4b5367', 'feba9ec0fd034765b11faa12c703fce1', '0e5625e96ca54f709321a683e941ab5a', 
    '5b2f71ff83d1435293fb3a5f57b18390')

