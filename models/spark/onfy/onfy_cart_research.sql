{{ config(
    schema='onfy',
    materialized='view',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

with cart_open as (
    select
        device_id,
        event_ts_cet,
        payload.carttab,
        payload.packagescount,
        payload.deliveryprice,
        payload.itemscount,
        payload.productscount,
        payload.productsprice,
        payload.deliveryprice + payload.productsprice as cart_price,
        payload.lowesttabdeliveryprice,
        payload.lowesttabfreedeliverypackages,
        payload.lowesttabpackagescount,
        payload.lowesttabproductsprice,
        payload.hasoptimalshippingtab,
        payload.optimalshippingdeliveryprice,
        payload.optimalshippingdisplayeddiscount,
        payload.optimalshippingproductsprice
    from {{ source('onfy_mart', 'device_events') }}
    where
        type = 'cartOpen'
        and DATE(event_ts_cet) >= '2023-06-01'
),

session_dates as (
    select
        device_id,
        session_minenv_dt,
        COALESCE(
            payment_dt,
            COALESCE(
                LEAD(session_minenv_dt) 
                    over (partition by device_id order by session_minenv_dt),
                (session_minenv_dt + INTERVAL 7 days)
            )
        ) as max_session_event_dt
    from {{ source('onfy', 'conversion_funnel') }}
    where
        window_size = '7 days'
        and DATE(session_minenv_dt) >= '2023-06-01'
),

cart_tab_switcher_events as (
    select
        device_id,
        event_ts_cet as switcher_dt
    from {{ source('onfy_mart', 'device_events') }}
    where
        type = 'optimalShippingSwitcherClick'
        and DATE(event_ts_cet) >= '2023-06-01'
),

cart_switcher as (
    select
        session_dates.device_id,
        session_dates.session_minenv_dt,
        COUNT(cart_tab_switcher_events.switcher_dt) as switcher_num
    from session_dates
    left join cart_tab_switcher_events 
        on 
            session_dates.device_id = cart_tab_switcher_events.device_id
        and
            cart_tab_switcher_events.switcher_dt 
                between session_dates.session_minenv_dt 
                and session_dates.max_session_event_dt
    group by 1, 2
),

sessions_last_cart as (
    select
        session_dates.*,
        cart_open.event_ts_cet as cart_dt,
        cart_open.carttab as last_carttab,
        cart_open.packagescount as last_packagescount,
        cart_open.deliveryprice as last_deliveryprice,
        cart_open.productsprice as last_productsprice,
        cart_open.hasoptimalshippingtab as last_hasoptimalshippingtab,
        cart_open.optimalshippingdeliveryprice as last_optimalshippingdeliveryprice,
        cart_open.optimalshippingproductsprice as fisrt_optimalshippingproductsprice,
        cart_open.optimalshippingdisplayeddiscount as fisrt_optimalshippingdisplayeddiscount,
        RANK() over (partition by session_dates.device_id,
                     session_dates.session_minenv_dt order by cart_open.event_ts_cet desc) as rnk
    from session_dates
    join cart_open 
        on session_dates.device_id = cart_open.device_id
        and session_dates.session_minenv_dt < cart_open.event_ts_cet
        and session_dates.max_session_event_dt > cart_open.event_ts_cet
),

max_status as (
    select
        session_dates.device_id,
        session_dates.session_minenv_dt,
        MAX(cart_open.packagescount) as max_parcels,
        MAX(cart_open.itemscount) as max_items,
        MAX(cart_open.cart_price) as max_cart,
        MAX(cart_open.hasoptimalshippingtab) as had_optimal
    from session_dates
    join cart_open
        on session_dates.device_id = cart_open.device_id
        and session_dates.session_minenv_dt < cart_open.event_ts_cet
        and session_dates.max_session_event_dt > cart_open.event_ts_cet
    group by 1, 2
),

funnel_cart as (
    select
        funnel.device_id,
        funnel.app_device_type,
        funnel.is_buyer,
        case
            when funnel.source like '%google%' then 'google'
            when funnel.source = 'idealo' then 'idealo'
            when funnel.source like '%facebook%' then 'facebook'
            when funnel.source = 'organic' then 'organic'
            else 'other'
        end as source,
        funnel.session_minenv_dt,
        funnel.cart_open_dt,
        funnel.payment_dt,
        session_dates.max_session_event_dt,
        cart_switcher.switcher_num,
  --------------------------------------------------------------------------------------------
        first_cart.packagescount as first_packagescount,
        case
            when first_cart.packagescount = 1 then '1 parcel'
            when first_cart.packagescount > 1 then '2 and more parcels'
        end as first_parcels_num,
        first_cart.carttab as first_carttab,
        first_cart.deliveryprice as first_deliveryprice,
        first_cart.productsprice as first_productsprice,
        first_cart.hasoptimalshippingtab as first_hasoptimalshippingtab,
        first_cart.optimalshippingdeliveryprice as first_optimalshippingdeliveryprice,
        first_cart.optimalshippingproductsprice as fisrt_optimalshippingproductsprice,
        first_cart.optimalshippingdisplayeddiscount as fisrt_optimalshippingdisplayeddiscount,
  --------------------------------------------------------------------------------------------
        last_cart.cart_dt as last_cart_dt,
        COALESCE(last_cart.last_packagescount, first_cart.packagescount) as last_packagescount,
        case 
            when COALESCE(last_cart.last_packagescount, first_cart.packagescount) = 1 then '1 parcel'
            when COALESCE(last_cart.last_packagescount, first_cart.packagescount) > 1 then '2 and more parcels'
        end as last_parcels_num,
        COALESCE(last_cart.last_carttab, first_cart.carttab) as last_carttab,
        last_cart.last_deliveryprice,
        last_cart.last_productsprice,
        last_cart.last_hasoptimalshippingtab,
        last_cart.last_optimalshippingdeliveryprice,
  -------------------------------------------------------------------------------------------- 
        max_status.max_parcels,
        max_status.max_items,
        max_status.max_cart,
        max_status.had_optimal
  from {{ source('onfy', 'conversion_funnel') }} funnel
  left join cart_open as first_cart
    on funnel.device_id = first_cart.device_id
    and funnel.cart_open_dt = first_cart.event_ts_cet 
  left join sessions_last_cart as last_cart
    on funnel.device_id = last_cart.device_id
    and funnel.session_minenv_dt = last_cart.session_minenv_dt 
    and last_cart.rnk = 1
  left join cart_switcher 
    on funnel.device_id = cart_switcher.device_id
    and funnel.session_minenv_dt = cart_switcher.session_minenv_dt
  left join max_status
    on funnel.device_id = max_status.device_id
    and funnel.session_minenv_dt = max_status.session_minenv_dt
  where funnel.window_size = '7 days'
    and date(funnel.session_minenv_dt) >= '2023-06-01'
    and funnel.cart_open_dt is not null
),

two_parcel_scenarios as (
    select
        *,
        case
            when funnel_cart.last_deliveryprice = 0 then 'free delivery'
            when funnel_cart.last_deliveryprice > 0 and funnel_cart.last_deliveryprice <= 5 then 'less then 5eur delivery'
            when funnel_cart.last_deliveryprice > 5 then 'more then 5eur delivery'
        end as delivery,
    GREATEST(funnel_cart.first_packagescount, funnel_cart.last_packagescount) as packages_count,
    case
        when 
            funnel_cart.switcher_num = 0
            and funnel_cart.max_parcels > 1
            and funnel_cart.had_optimal
            and funnel_cart.first_parcels_num = '1 parcel'
            and funnel_cart.last_parcels_num = '1 parcel'
        then 'manually_optimised'
        when 
            funnel_cart.switcher_num = 0
            and funnel_cart.first_carttab = 'optimalShipping' 
            and funnel_cart.last_carttab = 'optimalShipping'
        then 'optimal_pre_selected'
        when 
            funnel_cart.switcher_num = 0
            and funnel_cart.max_parcels > 1
            and not funnel_cart.had_optimal
            and funnel_cart.first_carttab != 'optimalShipping'
        then 'no_optimal_shipping'
        when 
            funnel_cart.switcher_num = 0
            and funnel_cart.max_parcels > 1
            and funnel_cart.had_optimal
            and funnel_cart.first_carttab != 'optimalShipping'
        then 'had_optimal_didnt_tried'
        when 
            funnel_cart.switcher_num > 0
            and funnel_cart.first_carttab != 'optimalShipping'
            and funnel_cart.last_carttab != 'optimalShipping' 
        then 'tried_optimal_didnt_use'
        when 
            funnel_cart.switcher_num > 0
            and funnel_cart.first_carttab != 'optimalShipping'
            and funnel_cart.last_carttab = 'optimalShipping' 
        then 'choose_optimal'
    end as cart_scenarios
from funnel_cart
)

select *
from two_parcel_scenarios