{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

with source as (
    select distinct
        *,
        (source_dt  - INTERVAL 1 minutes) as source_dt_minus_1min,
        (source_dt + INTERVAL 30 days) as source_dt_30d,
        (source_dt + INTERVAL 7 days) as source_dt_7d,
        (source_dt + INTERVAL 24 hours) as source_dt_24h
    from {{ ref('sources') }}

)

, devices_mart as (
    select
      device_id,
      app_device_type,
      min(min_purchase_date) as min_purchase_date
    from {{ source('onfy_mart', 'devices_mart') }}
    where not is_bot
    group by 1, 2
)

--------------------------------------------------------------------------------------------------------------------------
-- pre-selecting raw events
--------------------------------------------------------------------------------------------------------------------------

, session_start_raw as (
  select distinct
    device_id,
    event_ts_cet
  from {{ source('onfy_mart', 'device_events') }}
  where type in ('sessionConfigured', 'homeOpen')
)

, session_start as (
  select
    device_id,
    event_ts_cet as session_dt,
    (event_ts_cet + INTERVAL 30 days) as session_dt_30d,
    (event_ts_cet + INTERVAL 7 days) as session_dt_7d,
    (event_ts_cet + INTERVAL 24 hours) as session_dt_24h,
    lead(event_ts_cet) over(partition by device_id order by event_ts_cet) as next_session_dt 
  from session_start_raw
)

, minimal_involvement_temp as ( 
    select
      device_id,
      event_ts_cet as minimal_involvement_dt,
      case 
        when type = 'homeOpen' then 'main_page'
        when type in ('search', 'searchServer') then 'search'
        when type = 'productOpen' then 'product_page'
        when type = 'catalogOpen' then 'catalog'
        when type = 'productPreview' and payload.sourceScreen = 'productPageLanding' then 'products_list'
        else null 
        end as type
    from {{ source('onfy_mart', 'device_events') }}
    where type in ('search', 'searchServer', 'productOpen', 'catalogOpen', 'homeOpen', 'productPreview')
)

, minimal_involvement as (
  select * 
  from minimal_involvement_temp
  where type is not null
)

, add_to_cart as (
    select
      device_id,
      event_ts_cet as add_to_cart_dt
    from {{ source('onfy_mart', 'device_events') }}
    where type = 'addToCart'
)

, cart_open as (
    select
      device_id,
      event_ts_cet as cart_open_dt
    from {{ source('onfy_mart', 'device_events') }}
    where type = 'cartOpen'
      and payload.productIds is not null
)

, checkout_open as (
    select
      device_id,
      event_ts_cet as checkout_dt
    from {{ source('onfy_mart', 'device_events') }}
    where type = 'checkoutConfirmOpen'
)

, payment_start as (
    select
      device_id,
      event_ts_cet as payment_start_dt
    from {{ source('onfy_mart', 'device_events') }}
    where type = 'paymentStart'
)

, successful_payment as (
    select
      device_id,
      event_ts_cet as payment_dt,
      (event_ts_cet + INTERVAL 10 minutes) as payment_dt_10 --this is an only server event here so datetime of the event can be a little earlier
    from {{ source('onfy_mart', 'device_events') }}
    where type = 'paymentCompleteServer'
)

--------------------------------------------------------------------------------------------------------------------------
/*
30 days window

to cut "technical" session start events taking sessions only if:
 - previous session ended up with payment
 - it's been 30 days since last session 
 
+ taking all events within 30 days window from the first event
 
*/
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- adding sources to sessions
--------------------------------------------------------------------------------------------------------------------------

, sourced_sessions_30 as (
    select
        sessions.device_id,
        sessions.session_dt,
        sessions.session_dt_30d,
        next_session_dt,
        max_by(sources.source_corrected, sessions.session_dt) as source,
        max_by(sources.campaign_corrected, sessions.session_dt) as campaign,
        max_by(sources.partner, sessions.session_dt) as partner
    from session_start as sessions
    left join source as sources
        on sessions.device_id = sources.device_id
        and sessions.session_dt between source_dt_minus_1min and coalesce(next_source_dt, to_timestamp('2100-01-01 00:00:00'))
    group by
        sessions.device_id,
        sessions.session_dt,
        sessions.session_dt_30d,
        next_session_dt
)

--------------------------------------------------------------------------------------------------------------------------
-- only taking sessions with minimal involvement - to cut empty sessions, checking order status sessions and so on
--------------------------------------------------------------------------------------------------------------------------

, minimal_involvement_sessions_30_raw as (
  select distinct
    sessions.device_id,
    session_dt,
    session_dt_30d,
    sessions.source,
    sessions.campaign,
    sessions.partner,
    minimal_involvement.type,
    rank() over(partition by minimal_involvement.device_id, session_dt order by minimal_involvement_dt) as type_rnk
  from sourced_sessions_30 as sessions
  join minimal_involvement
    on sessions.device_id = minimal_involvement.device_id
    and minimal_involvement_dt between session_dt and least(coalesce(next_session_dt, to_timestamp('2100-01-01 00:00:00')), session_dt_30d)
)

, minimal_involvement_sessions_30 as (
    select distinct
      device_id,
      session_dt as session_minenv_dt,
      session_dt_30d,
      source,
      campaign,
      partner,
      type,
      lag(session_dt) over(partition by device_id order by session_dt) as prev_session_minenv_dt
      , lead(session_dt) over(partition by device_id order by session_dt) as next_session_minenv_dt
    from minimal_involvement_sessions_30_raw
    where type_rnk = 1
)

, sessions_with_payments_30 as (
    select
      sessions.*,
      payment_dt,
      datediff(session_minenv_dt, prev_session_minenv_dt) as session_diff_days,
      floor((bigint(session_minenv_dt) -  bigint(prev_session_minenv_dt))/3600) as session_diff_hours,
      row_number() over(partition by sessions.device_id, session_minenv_dt order by payment_dt desc) as rn
    from minimal_involvement_sessions_30 as sessions
    left join successful_payment as payment
      on sessions.device_id = payment.device_id
      and payment_dt between session_minenv_dt and least(coalesce(next_session_minenv_dt, to_timestamp('2100-01-01 00:00:00')), session_dt_30d)
)

, sessions_counter_30 as (
    select
      *,
      case
        when
          prev_session_minenv_dt is null
          then 1
        when
          prev_session_minenv_dt is not null
          and session_diff_days >= 30
          then 1
        when
          prev_session_minenv_dt is not null
          and session_diff_days < 30
          and payment_dt is not null
          then 1
        else 0
        end as is_session
    from sessions_with_payments_30
    where rn=1
)

, actual_sessions_30 as (
    select
      device_id,
      session_minenv_dt,
      (session_minenv_dt + INTERVAL 30 days) as session_window_30d, -- we use this to have events chain exactly in 30d window
      type,
      source,
      campaign,
      partner
    from sessions_counter_30
    where is_session = 1
)

--------------------------------------------------------------------------------------------------------------------------
-- collecting events into the events chain with 30d window
--------------------------------------------------------------------------------------------------------------------------

, minim_env_add_to_cart_30 as (
    select
      session.device_id,
      session_minenv_dt,
      session_window_30d,
      session.type,
      session.source,
      session.campaign,
      session.partner,
      add_to_cart_dt,
      row_number() over(partition by session.device_id, session_minenv_dt order by add_to_cart_dt) as rnk_add_to_cart
    from actual_sessions_30 session
    left join add_to_cart add_cart
      on session.device_id = add_cart.device_id
      and add_to_cart_dt between session_minenv_dt and session_window_30d
)

, add_to_cart_cart_open_30 as (
    select
      minim_env_add_to_cart_30.*,
      cart_open_dt,
      row_number() over(partition by minim_env_add_to_cart_30.device_id, session_minenv_dt, add_to_cart_dt order by cart_open_dt) as rnk_cart_open
    from minim_env_add_to_cart_30
    left join cart_open
      on minim_env_add_to_cart_30.device_id = cart_open.device_id
      and cart_open_dt between add_to_cart_dt and session_window_30d
    where rnk_add_to_cart=1
)

, cart_open_to_checkout_30 as (
    select
      add_to_cart_cart_open_30.*,
      checkout_dt,
      row_number() over(partition by add_to_cart_cart_open_30.device_id, session_minenv_dt, cart_open_dt order by checkout_dt) as rnk_checkout
    from add_to_cart_cart_open_30
    left join checkout_open
      on add_to_cart_cart_open_30.device_id = checkout_open.device_id
      and checkout_dt between cart_open_dt and session_window_30d
    where rnk_cart_open=1
)

, checkout_to_payment_start_30 as (
    select
      cart_open_to_checkout_30.*,
      payment_start_dt,
      row_number() over(partition by cart_open_to_checkout_30.device_id, session_minenv_dt, checkout_dt order by payment_start_dt) as rnk_payment_start
    from cart_open_to_checkout_30
    left join payment_start
      on cart_open_to_checkout_30.device_id = payment_start.device_id
      and payment_start_dt between checkout_dt and session_window_30d
    where rnk_checkout = 1
)

, payment_start_to_payment_30 as (
    select
      checkout_to_payment_start_30.*,
      payment_dt,
      row_number() over(partition by checkout_to_payment_start_30.device_id, session_minenv_dt, payment_start_dt order by payment_dt) as rnk_payment
    from checkout_to_payment_start_30
    left join successful_payment
      on checkout_to_payment_start_30.device_id = successful_payment.device_id
      and payment_dt_10 between payment_start_dt and session_window_30d
    where rnk_payment_start = 1
)

-----------------------------------------------------------------------------------------
/*
7 days window

to cut "technical" session start events taking sessions only if:
 - previous session ended up with payment
 - it's been 7 days since last session 
 
+ taking all events within 7 days window from the first event 
 
*/
-----------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- adding sources to sessions
--------------------------------------------------------------------------------------------------------------------------

, sourced_sessions_7 as (
    select
        sessions.device_id,
        sessions.session_dt,
        sessions.session_dt_7d,
        next_session_dt,
        max_by(sources.source_corrected, sessions.session_dt) as source,
        max_by(sources.campaign_corrected, sessions.session_dt) as campaign,
        max_by(sources.partner, sessions.session_dt) as partner
    from session_start as sessions
    left join source as sources
        on sessions.device_id = sources.device_id
        and sessions.session_dt between source_dt_minus_1min and coalesce(next_source_dt, to_timestamp('2100-01-01 00:00:00'))
    group by
        sessions.device_id,
        sessions.session_dt,
        sessions.session_dt_7d,
        next_session_dt
)

--------------------------------------------------------------------------------------------------------------------------
-- only taking sessions with minimal involvement - to cut empty sessions, checking order status sessions and so on
--------------------------------------------------------------------------------------------------------------------------

, minimal_involvement_sessions_7_raw as (
select distinct
  sessions.device_id,
  session_dt,
  session_dt_7d,
  sessions.source,
  sessions.campaign,
  sessions.partner,
  minimal_involvement.type,
  rank() over(partition by minimal_involvement.device_id, session_dt order by minimal_involvement_dt) as type_rnk
from sourced_sessions_7 as sessions
join minimal_involvement
  on sessions.device_id = minimal_involvement.device_id
  and minimal_involvement_dt between session_dt and least(coalesce(next_session_dt, to_timestamp('2100-01-01 00:00:00')), session_dt_7d)
)

, minimal_involvement_sessions_7 as (
select distinct
  device_id,
  session_dt as session_minenv_dt,
  session_dt_7d,
  source,
  campaign,
  partner,
  type,
  lag(session_dt) over(partition by device_id order by session_dt) as prev_session_minenv_dt
  , lead(session_dt) over(partition by device_id order by session_dt) as next_session_minenv_dt
from minimal_involvement_sessions_7_raw
where type_rnk=1
)

, sessions_with_payments_7 as (
    select
      sessions.*,
      payment_dt,
      datediff(session_minenv_dt, prev_session_minenv_dt) as session_diff_days,
      floor((bigint(session_minenv_dt) -  bigint(prev_session_minenv_dt))/3600) as session_diff_hours,
      row_number() over(partition by sessions.device_id, session_minenv_dt order by payment_dt desc) as rn
    from minimal_involvement_sessions_7 as sessions
    left join successful_payment as payment
      on sessions.device_id = payment.device_id
      and payment_dt between session_minenv_dt and least(coalesce(next_session_minenv_dt, to_timestamp('2100-01-01 00:00:00')), session_dt_7d)
)

, sessions_counter_7 as (
    select
      *,
      case
        when
          prev_session_minenv_dt is null
          then 1
        when
          prev_session_minenv_dt is not null
          and session_diff_days >= 7
          then 1
        when
          prev_session_minenv_dt is not null
          and session_diff_days < 7
          and payment_dt is not null
          then 1
        else 0
        end as is_session
    from sessions_with_payments_7
    where rn=1
)

, actual_sessions_7 as (
    select
      device_id,
      session_minenv_dt,
      (session_minenv_dt + INTERVAL 7 days) as session_window_7d, -- we use this to have events chain exactly in 7d window
      type,
      source,
      campaign,
      partner
    from sessions_counter_7
    where is_session = 1
)

--------------------------------------------------------------------------------------------------------------------------
-- collecting events into the events chain with 7d window
--------------------------------------------------------------------------------------------------------------------------

, minim_env_add_to_cart_7 as (
    select
      session.device_id,
      session_minenv_dt,
      session_window_7d,
      session.source,
      session.type,
      session.campaign,
      session.partner,
      add_to_cart_dt,
      row_number() over(partition by session.device_id, session_minenv_dt order by add_to_cart_dt) as rnk_add_to_cart
    from actual_sessions_7 session
    left join add_to_cart add_cart
      on session.device_id = add_cart.device_id
      and add_to_cart_dt between session_minenv_dt and session_window_7d
)

, add_to_cart_cart_open_7 as (
    select
      minim_env_add_to_cart_7.*,
      cart_open_dt,
      row_number() over(partition by minim_env_add_to_cart_7.device_id, session_minenv_dt, add_to_cart_dt order by cart_open_dt) as rnk_cart_open
    from minim_env_add_to_cart_7
    left join cart_open
      on minim_env_add_to_cart_7.device_id = cart_open.device_id
      and cart_open_dt between add_to_cart_dt and session_window_7d
    where rnk_add_to_cart=1
)

, cart_open_to_checkout_7 as (
    select
      add_to_cart_cart_open_7.*,
      checkout_dt,
      row_number() over(partition by add_to_cart_cart_open_7.device_id, session_minenv_dt, cart_open_dt order by checkout_dt) as rnk_checkout
    from add_to_cart_cart_open_7
    left join checkout_open
      on add_to_cart_cart_open_7.device_id = checkout_open.device_id
      and checkout_dt between cart_open_dt and session_window_7d
    where rnk_cart_open=1
)

, checkout_to_payment_start_7 as (
    select
      cart_open_to_checkout_7.*,
      payment_start_dt,
      row_number() over(partition by cart_open_to_checkout_7.device_id, session_minenv_dt, checkout_dt order by payment_start_dt) as rnk_payment_start
    from cart_open_to_checkout_7
    left join payment_start
      on cart_open_to_checkout_7.device_id = payment_start.device_id
      and payment_start_dt between checkout_dt and session_window_7d
    where rnk_checkout = 1
)

, payment_start_to_payment_7 as (
    select
      checkout_to_payment_start_7.*,
      payment_dt,
      row_number() over(partition by checkout_to_payment_start_7.device_id, session_minenv_dt, payment_start_dt order by payment_dt) as rnk_payment
    from checkout_to_payment_start_7
    left join successful_payment
      on checkout_to_payment_start_7.device_id = successful_payment.device_id
      and payment_dt_10 between payment_start_dt and session_window_7d
    where rnk_payment_start = 1
)

-----------------------------------------------------------------------------------------
/*
24 hours window

to cut "technical" session start events taking sessions only if:
 - previous session ended up with payment
 - it's been 24 hours since last session 
 
+ taking all events within 24 hours window from the first event

*/
-----------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- adding sources to sessions
--------------------------------------------------------------------------------------------------------------------------

, sourced_sessions_24 as (
    select
        sessions.device_id,
        sessions.session_dt,
        sessions.session_dt_24h,
        next_session_dt,
        max_by(sources.source_corrected, sessions.session_dt) as source,
        max_by(sources.campaign_corrected, sessions.session_dt) as campaign,
        max_by(sources.partner, sessions.session_dt) as partner
    from session_start as sessions
    left join source as sources
        on sessions.device_id = sources.device_id
        and sessions.session_dt between source_dt_minus_1min and coalesce(next_source_dt, to_timestamp('2100-01-01 00:00:00'))
    group by
        sessions.device_id,
        sessions.session_dt,
        sessions.session_dt_24h,
        next_session_dt
)

--------------------------------------------------------------------------------------------------------------------------
-- only taking sessions with minimal involvement - to cut empty sessions, checking order status sessions and so on
--------------------------------------------------------------------------------------------------------------------------


, minimal_involvement_sessions_24_raw as (
select distinct
  sessions.device_id,
  session_dt as session_minenv_dt,
  session_dt_24h,
  sessions.source,
  sessions.campaign,
  sessions.partner,
  minimal_involvement.type,
  rank() over(partition by minimal_involvement.device_id, session_dt order by minimal_involvement_dt) as type_rnk
from sourced_sessions_24 as sessions
join minimal_involvement
  on sessions.device_id = minimal_involvement.device_id
  and minimal_involvement_dt between session_dt and least(coalesce(next_session_dt, to_timestamp('2100-01-01 00:00:00')), session_dt_24h)
)

, minimal_involvement_sessions_24 as (
select distinct
  *,
  lag(session_minenv_dt) over(partition by device_id order by session_minenv_dt) as prev_session_minenv_dt
  , lead(session_minenv_dt) over(partition by device_id order by session_minenv_dt) as next_session_minenv_dt
from minimal_involvement_sessions_24_raw
where type_rnk=1
)

, sessions_with_payments_24 as (
    select
      sessions.*,
      payment_dt,
      datediff(session_minenv_dt, prev_session_minenv_dt) as session_diff_days,
      floor((bigint(session_minenv_dt) -  bigint(prev_session_minenv_dt))/3600) as session_diff_hours,
      row_number() over(partition by sessions.device_id, session_minenv_dt order by payment_dt desc) as rn
    from minimal_involvement_sessions_24 as sessions
    left join successful_payment as payment
      on sessions.device_id = payment.device_id
      and payment_dt between session_minenv_dt and least(coalesce(next_session_minenv_dt, to_timestamp('2100-01-01 00:00:00')), session_dt_24h)
    where type_rnk = 1 
)

, sessions_counter_24 as (
    select
      *,
      case
        when
          prev_session_minenv_dt is null
          then 1
        when
          prev_session_minenv_dt is not null
          and session_diff_hours >=24
          then 1
        when
          prev_session_minenv_dt is not null
          and session_diff_hours < 24
          and payment_dt is not null
          then 1
        else 0
        end as is_session
    from sessions_with_payments_24
    where rn=1
)

, actual_sessions_24 as (
    select
      device_id,
      session_minenv_dt,
      (session_minenv_dt + INTERVAL 24 hours) as session_window_24h, -- we use this to have events chain exactly in 24h window
      type,
      source,
      campaign,
      partner
    from sessions_counter_24
    where is_session = 1
)

--------------------------------------------------------------------------------------------------------------------------
-- collecting events into the events chain with 24h window
--------------------------------------------------------------------------------------------------------------------------

, minim_env_add_to_cart_24 as (
    select
      session.device_id,
      session_minenv_dt,
      session_window_24h,
      session.type,
      session.source,
      session.campaign,
      session.partner,
      add_to_cart_dt,
      row_number() over(partition by session.device_id, session_minenv_dt order by add_to_cart_dt) as rnk_add_to_cart
    from actual_sessions_24 session
    left join add_to_cart add_cart
      on session.device_id = add_cart.device_id
      and add_to_cart_dt between session_minenv_dt and session_window_24h
)

, add_to_cart_cart_open_24 as (
    select
      minim_env_add_to_cart_24.*,
      cart_open_dt,
      row_number() over(partition by minim_env_add_to_cart_24.device_id, session_minenv_dt, add_to_cart_dt order by cart_open_dt) as rnk_cart_open
    from minim_env_add_to_cart_24
    left join cart_open
      on minim_env_add_to_cart_24.device_id = cart_open.device_id
      and cart_open_dt between add_to_cart_dt and session_window_24h
    where rnk_add_to_cart=1
)

, cart_open_to_checkout_24 as (
    select
      add_to_cart_cart_open_24.*,
      checkout_dt,
      row_number() over(partition by add_to_cart_cart_open_24.device_id, session_minenv_dt, cart_open_dt order by checkout_dt) as rnk_checkout
    from add_to_cart_cart_open_24
    left join checkout_open
      on add_to_cart_cart_open_24.device_id = checkout_open.device_id
      and checkout_dt between cart_open_dt and session_window_24h
    where rnk_cart_open=1
)

, checkout_to_payment_start_24 as (
    select
      cart_open_to_checkout_24.*,
      payment_start_dt,
      row_number() over(partition by cart_open_to_checkout_24.device_id, session_minenv_dt, checkout_dt order by payment_start_dt) as rnk_payment_start
    from cart_open_to_checkout_24
    left join payment_start
      on cart_open_to_checkout_24.device_id = payment_start.device_id
      and payment_start_dt between checkout_dt and session_window_24h
    where rnk_checkout = 1
)

, payment_start_to_payment_24 as (
    select
      checkout_to_payment_start_24.*,
      payment_dt,
      row_number() over(partition by checkout_to_payment_start_24.device_id, session_minenv_dt, payment_start_dt order by payment_dt) as rnk_payment
    from checkout_to_payment_start_24
    left join successful_payment
      on checkout_to_payment_start_24.device_id = successful_payment.device_id
      and payment_dt_10 between payment_start_dt and session_window_24h
    where rnk_payment_start = 1
)

--------------------------------------------------------------------------------------------------------------------------
-- collecting all together
--------------------------------------------------------------------------------------------------------------------------

, all_together as (
    select
        events_30.device_id,
        events_30.session_minenv_dt,
        events_30.add_to_cart_dt,
        events_30.cart_open_dt,
        events_30.checkout_dt,
        events_30.payment_start_dt,
        events_30.payment_dt,
        events_30.type,
        events_30.source,
        events_30.campaign,
        events_30.partner,
        '30 days' as window_size
    from payment_start_to_payment_30 as events_30
    where events_30.rnk_payment = 1

union all

    select
        events_7.device_id,
        events_7.session_minenv_dt,
        events_7.add_to_cart_dt,
        events_7.cart_open_dt,
        events_7.checkout_dt,
        events_7.payment_start_dt,
        events_7.payment_dt,
        events_7.type,
        events_7.source,
        events_7.campaign,
        events_7.partner,
        '7 days' as window_size
    from payment_start_to_payment_7 as events_7
    where events_7.rnk_payment = 1

union all

    select
        events_1.device_id,
        events_1.session_minenv_dt,
        events_1.add_to_cart_dt,
        events_1.cart_open_dt,
        events_1.checkout_dt,
        events_1.payment_start_dt,
        events_1.payment_dt,
        events_1.type,
        events_1.source,
        events_1.campaign,
        events_1.partner,
        '24 hours' as window_size
    from payment_start_to_payment_24 as events_1
    where events_1.rnk_payment = 1
)
select 
    all_together.device_id,
    devices.app_device_type,
    case 
      when date(all_together.session_minenv_dt) > devices.min_purchase_date then True
      else False 
      end as is_buyer,
    session_minenv_dt,
    add_to_cart_dt,
    cart_open_dt,
    checkout_dt,
    payment_start_dt,
    payment_dt,
    source,
    campaign,
    partner,
    window_size,
    type as session_start_screen
from all_together
inner join devices_mart as devices
  on all_together.device_id = devices.device_id
