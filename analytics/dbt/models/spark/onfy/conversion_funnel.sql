{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

with session_start as (
  select distinct
    device_id,
    event_ts_cet as session_dt,
    lead(event_ts_cet) over(partition by device_id order by event_ts_cet) as next_session_dt 
  from {{ source('onfy_mart', 'device_events')}}
  where type in ('sessionConfigured', 'homeOpen')
)
, minimal_envolvement as ( 
select 
  device_id,
  event_ts_cet as minimal_envolvement_dt,
  type
from {{ source('onfy_mart', 'device_events')}}
where type in ('search', 'searchServer', 'productOpen', 'catalogOpen')
)
, add_to_cart as (
select 
  device_id,
  event_ts_cet as add_to_cart_dt
from {{ source('onfy_mart', 'device_events')}}
where type = 'addToCart'
)
, cart_open as (
select 
  device_id,
  event_ts_cet as cart_open_dt
from {{ source('onfy_mart', 'device_events')}}
where type = 'cartOpen'
  and payload.productIds is not null 
)
, checkout_open as (
select 
  device_id,
  event_ts_cet as checkout_dt
from {{ source('onfy_mart', 'device_events')}}
where type = 'checkoutConfirmOpen'
)
, payment_start as (
select 
  device_id,
  event_ts_cet as payment_start_dt
from {{ source('onfy_mart', 'device_events')}}
where type = 'paymentStart'
)
, successful_payment as (
select 
  device_id,
  event_ts_cet as payment_dt
from {{ source('onfy_mart', 'device_events')}}
where type = 'paymentCompleteServer'
)
-----------------------------------------------------------------------------------------
-- only taking sessions with minimal envolvement - to cut empty sessions, checking order status sessions and so on
-----------------------------------------------------------------------------------------
, minimal_envolvement_sessions as (
select distinct
  sessions.device_id,
  session_dt as session_minenv_dt,
  lag(session_dt) over(partition by sessions.device_id order by session_dt) as prev_session_minenv_dt
  , lead(session_dt) over(partition by sessions.device_id order by session_dt) as next_session_minenv_dt
from session_start as sessions
join minimal_envolvement
  on sessions.device_id = minimal_envolvement.device_id
  and minimal_envolvement_dt between session_dt and coalesce(next_session_dt, CURRENT_TIMESTAMP())
)
-----------------------------------------------------------------------------------------
/*
to cut "technical" session start events taking sessions only if:
 - previous session ended up with payment
 - it's been 30 days since last session 
*/
-----------------------------------------------------------------------------------------
, sessions_with_payments as (
select 
  sessions.*,
  payment_dt,
  datediff(session_minenv_dt, prev_session_minenv_dt) as session_diff_days, 
  row_number() over(partition by sessions.device_id, session_minenv_dt order by payment_dt desc) as rn
from minimal_envolvement_sessions as sessions 
left join successful_payment as payment
  on sessions.device_id = payment.device_id
  and payment_dt between session_minenv_dt and coalesce(next_session_minenv_dt, CURRENT_TIMESTAMP())
)
, sessions_counter as (
select 
  *,
  case 
    when 
      prev_session_minenv_dt is null then 1 
    when 
      prev_session_minenv_dt is not null
      and session_diff_days >= 30 then 1
    when 
      prev_session_minenv_dt is not null
      and session_diff_days < 30 
      and payment_dt is not null 
      and rn=1 then 1
    else 0 end as is_session 
from sessions_with_payments
)
, actual_sessions as (
select 
  device_id,
  session_minenv_dt 
from sessions_counter
where is_session = 1
)
-----------------------------------------------------------------------------------------
-- joining events with each other
-----------------------------------------------------------------------------------------
, minim_env_add_to_cart as (
select  
  session.device_id,
  session_minenv_dt,
  add_to_cart_dt,
  row_number() over(partition by session.device_id, session_minenv_dt order by add_to_cart_dt) as rnk_add_to_cart
from actual_sessions session 
left join add_to_cart add_cart 
  on session.device_id = add_cart.device_id
  and add_to_cart_dt >= session_minenv_dt 
)
-----------------------------------------------------------------------------------------
, add_to_cart_cart_open as (
select 
  minim_env_add_to_cart.*,
  cart_open_dt,
  row_number() over(partition by minim_env_add_to_cart.device_id, add_to_cart_dt order by cart_open_dt) as rnk_cart_open
from minim_env_add_to_cart  
left join cart_open 
  on minim_env_add_to_cart.device_id = cart_open.device_id
  and cart_open_dt >= add_to_cart_dt --TIMESTAMP_SUB(add_to_cart_dt, interval 10 minute)
where rnk_add_to_cart=1
)
-----------------------------------------------------------------------------------------
, cart_open_to_checkout as (
select 
  cart_open.*,
  checkout_dt,
  row_number() over(partition by cart_open.device_id, cart_open_dt order by checkout_dt) as rnk_checkout
from add_to_cart_cart_open cart_open
left join checkout_open  
  on cart_open.device_id = checkout_open.device_id
  and checkout_dt >=  cart_open_dt
where rnk_cart_open=1
)
-----------------------------------------------------------------------------------------
, checkout_to_payment_start as (
select 
  cart_open_to_checkout.*,
  payment_start_dt,
  row_number() over(partition by cart_open_to_checkout.device_id, checkout_dt order by payment_start_dt) as rnk_payment_start
from cart_open_to_checkout
left join payment_start
  on cart_open_to_checkout.device_id = payment_start.device_id
  and payment_start_dt >= checkout_dt
where rnk_checkout = 1
)
-----------------------------------------------------------------------------------------
, payment_start_to_payment as (
select 
  checkout_to_payment_start.*,
  payment_dt,
  row_number() over(partition by checkout_to_payment_start.device_id, payment_start_dt order by payment_dt) as rnk_payment
from checkout_to_payment_start
left join successful_payment
  on checkout_to_payment_start.device_id = successful_payment.device_id
  and payment_dt >= (payment_start_dt - INTERVAL 10 minutes) -- payment is a server event so it can be sent earlier than other events (which are client ones)
where rnk_payment_start = 1 
)
-----------------------------------------------------------------------------------------
, devices_mart as (
select 
  device_id,
  app_device_type,
  min(min_purchase_date) as min_purchase_date
from {{ source('onfy_mart', 'devices_mart')}}
where not is_bot
group by 1, 2
)
select 
    events.device_id,
    devices.app_device_type,
    case 
      when date(events.session_minenv_dt) > devices.min_purchase_date then True
      else False 
      end as is_buyer,
    session_minenv_dt,
    add_to_cart_dt,
    cart_open_dt,
    checkout_dt,
    payment_start_dt,
    payment_dt
from payment_start_to_payment as events
inner join devices_mart as devices
  on events.device_id = devices.device_id
where rnk_payment = 1