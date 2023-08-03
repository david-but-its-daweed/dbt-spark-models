{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}


with uid as
(
    select
        device_id,
        app_device_type,
        min_by(user_email_hash, min_purchase_date) as user_email_hash
    from {{source('onfy_mart', 'devices_mart')}}
    where 1=1
        and is_bot = False
    group by 
        device_id,
        app_device_type
),

corrected_sources as 
(
    select *
    from 
        {{source('onfy', 'sources')}}
),

session_precalc as 
(
    select 
        type,
        device_id,
        source_dt,
        lead(source_dt) over (partition by device_id order by source_dt) as next_event,
        lag(source_dt) over (partition by device_id order by source_dt) as previous_event,
        if(
            to_unix_timestamp(source_dt) - to_unix_timestamp(lag(source_dt) over (partition by device_id order by source_dt)) <= 60*60*24
            and to_unix_timestamp(lag(source_dt) over (partition by device_id order by source_dt)) is not null,
            0, 1
        ) as new_session_group,
        if(source_corrected in ('unknown', 'unmarked_facebook_or_instagram', 'social'), 0, 1) as significant_source,
        source_corrected,
        campaign_corrected
    from corrected_sources
    where 1=1
),

sessions_window as 
(
    select 
        type,
        source_corrected,
        campaign_corrected,
        device_id,
        source_dt,
        next_event,
        previous_event,
        sum(significant_source) over (partition by device_id order by source_dt) as significant_source_window,
        sum(new_session_group) over (partition by device_id order by source_dt) as timeout_source_window,
        sum(new_session_group * significant_source) over (partition by device_id order by source_dt) as ultimate_window
    from session_precalc
),

sessions as 
(
    select 
        *,
        first_value(sessions_window.source_corrected) over (partition by device_id, ultimate_window order by source_dt) as source,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, ultimate_window order by source_dt) as campaign,
        first_value(sessions_window.source_corrected) over (partition by device_id, significant_source_window order by source_dt) as source_significant,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, significant_source_window order by source_dt) as campaign_significant,
        rank() over (partition by device_id, ultimate_window order by source_dt) as session_num
    from sessions_window
),

order_data AS 
(
    select
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.id as order_id,
        pharmacy_landing.order.payment_method,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') as order_created_time_cet,
        date_trunc('DAY', from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')) as order_created_date_cet,
        pharmacy_landing.order.service_fee_price,
        coalesce(pharmacy_landing.refund.service_fee_refund_price, 0) as service_fee_refund_price,
        sum(
            cast(
                pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity 
                - coalesce(pharmacy_landing.order_parcel_item.total_price_after_discount_price, pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity) 
            as double) 
        ) as promocode_discount,
        count(pharmacy_landing.order.id) over (partition by pharmacy_landing.order.user_email_hash order by from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')) as order_num
    from {{ source('pharmacy_landing', 'order') }}
    join {{ source('pharmacy_landing', 'order_parcel') }}
        on pharmacy_landing.order.id = pharmacy_landing.order_parcel.order_id    
    join {{ source('pharmacy_landing', 'order_parcel_item') }}
        on pharmacy_landing.order_parcel_item.order_parcel_id = pharmacy_landing.order_parcel.id
        and pharmacy_landing.order_parcel_item.product_id is not null
    left join {{ source('pharmacy_landing', 'refund') }}
        on pharmacy_landing.refund.order_id = pharmacy_landing.order.id
    where 1=1
    group by 
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.id,
        pharmacy_landing.order.payment_method,
        order_created_time_cet,
        order_created_date_cet,
        pharmacy_landing.order.service_fee_price,
        service_fee_refund_price
),


data_combined as
(
    select 
      source_dt as session_dt,
      date_trunc('month', coalesce(source_dt, order_created_time_cet)) as report_month,
      coalesce(source_dt, order_created_time_cet) as report_dt,
      coalesce(sessions.source, 'direct') as source,
      sessions.source_significant,
      sessions.campaign,
      sessions.campaign_significant,
      sessions.ultimate_window,
      sessions.significant_source_window,
      sessions.timeout_source_window,
      coalesce(sessions.device_id, order_data.device_id) as device_id,
      coalesce(order_data.user_email_hash, sessions.device_id) as combined_id,
      promocode_discount,
      order_id,
      session_num,
      order_num,
      payment_method,
      order_created_time_cet as order_dt,
      app_device_type
    from sessions
    full join order_data 
      on order_data.device_id = sessions.device_id 
      and order_data.order_created_time_cet between sessions.source_dt and coalesce(sessions.next_event, sessions.source_dt + interval 5 hours)
    join uid 
        on coalesce(sessions.device_id, order_data.device_id) = uid.device_id
)


select 
    *
from 
    data_combined  
