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

WITH cart_open AS (
    SELECT
        device_id,
        event_ts_cet,
        payload.carttab,
        payload.packagescount,
        payload.deliveryprice,
        payload.itemscount,
        payload.productscount,
        payload.productsprice,
        payload.deliveryprice + payload.productsprice AS cart_price,
        payload.lowesttabdeliveryprice,
        payload.lowesttabfreedeliverypackages,
        payload.lowesttabpackagescount,
        payload.lowesttabproductsprice,
        payload.hasoptimalshippingtab,
        payload.optimalshippingdeliveryprice,
        payload.optimalshippingdisplayeddiscount,
        payload.optimalshippingproductsprice
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        type = 'cartOpen'
        AND DATE(event_ts_cet) >= '2023-06-01'
),

session_dates AS (
    SELECT
        device_id,
        session_minenv_dt,
        COALESCE(
            payment_dt,
            COALESCE(
                LEAD(session_minenv_dt)
                OVER (PARTITION BY device_id ORDER BY session_minenv_dt),
                (session_minenv_dt + INTERVAL 7 DAYS)
            )
        ) AS max_session_event_dt
    FROM {{ ref('conversion_funnel') }}
    WHERE
        window_size = '7 days'
        AND DATE(session_minenv_dt) >= '2023-06-01'
),

cart_tab_switcher_events AS (
    SELECT
        device_id,
        event_ts_cet AS switcher_dt
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        type = 'optimalShippingSwitcherClick'
        AND DATE(event_ts_cet) >= '2023-06-01'
),

cart_switcher AS (
    SELECT
        session_dates.device_id,
        session_dates.session_minenv_dt,
        COUNT(cart_tab_switcher_events.switcher_dt) AS switcher_num
    FROM session_dates
    LEFT JOIN cart_tab_switcher_events
        ON
            session_dates.device_id = cart_tab_switcher_events.device_id
            AND
            cart_tab_switcher_events.switcher_dt
            BETWEEN session_dates.session_minenv_dt
            AND session_dates.max_session_event_dt
    GROUP BY 1, 2
),

sessions_last_cart AS (
    SELECT
        session_dates.*,
        cart_open.event_ts_cet AS cart_dt,
        cart_open.carttab AS last_carttab,
        cart_open.packagescount AS last_packagescount,
        cart_open.deliveryprice AS last_deliveryprice,
        cart_open.productsprice AS last_productsprice,
        cart_open.hasoptimalshippingtab AS last_hasoptimalshippingtab,
        cart_open.optimalshippingdeliveryprice AS last_optimalshippingdeliveryprice,
        cart_open.optimalshippingproductsprice AS fisrt_optimalshippingproductsprice,
        cart_open.optimalshippingdisplayeddiscount AS fisrt_optimalshippingdisplayeddiscount,
        RANK() OVER (
            PARTITION BY
                session_dates.device_id,
                session_dates.session_minenv_dt
            ORDER BY cart_open.event_ts_cet DESC
        ) AS rnk
    FROM session_dates
    INNER JOIN cart_open
        ON
            session_dates.device_id = cart_open.device_id
            AND session_dates.session_minenv_dt < cart_open.event_ts_cet
            AND session_dates.max_session_event_dt > cart_open.event_ts_cet
),

max_status AS (
    SELECT
        session_dates.device_id,
        session_dates.session_minenv_dt,
        MAX(cart_open.packagescount) AS max_parcels,
        MAX(cart_open.itemscount) AS max_items,
        MAX(cart_open.cart_price) AS max_cart,
        MAX(cart_open.hasoptimalshippingtab) AS had_optimal
    FROM session_dates
    INNER JOIN cart_open
        ON
            session_dates.device_id = cart_open.device_id
            AND session_dates.session_minenv_dt < cart_open.event_ts_cet
            AND session_dates.max_session_event_dt > cart_open.event_ts_cet
    GROUP BY 1, 2
),

funnel_cart AS (
    SELECT
        funnel.device_id,
        funnel.app_device_type,
        funnel.is_buyer,
        CASE
            WHEN funnel.source LIKE '%google%' THEN 'google'
            WHEN funnel.source = 'idealo' THEN 'idealo'
            WHEN funnel.source LIKE '%facebook%' THEN 'facebook'
            WHEN funnel.source = 'organic' THEN 'organic'
            ELSE 'other'
        END AS source,
        funnel.session_minenv_dt,
        funnel.cart_open_dt,
        funnel.payment_dt,
        last_cart.max_session_event_dt,
        cart_switcher.switcher_num,
        --------------------------------------------------------------------------------------------
        first_cart.packagescount AS first_packagescount,
        CASE
            WHEN first_cart.packagescount = 1 THEN '1 parcel'
            WHEN first_cart.packagescount > 1 THEN '2 and more parcels'
        END AS first_parcels_num,
        first_cart.carttab AS first_carttab,
        first_cart.deliveryprice AS first_deliveryprice,
        first_cart.productsprice AS first_productsprice,
        first_cart.hasoptimalshippingtab AS first_hasoptimalshippingtab,
        first_cart.optimalshippingdeliveryprice AS first_optimalshippingdeliveryprice,
        first_cart.optimalshippingproductsprice AS fisrt_optimalshippingproductsprice,
        first_cart.optimalshippingdisplayeddiscount AS fisrt_optimalshippingdisplayeddiscount,
        --------------------------------------------------------------------------------------------
        last_cart.cart_dt AS last_cart_dt,
        COALESCE(last_cart.last_packagescount, first_cart.packagescount) AS last_packagescount,
        CASE
            WHEN COALESCE(last_cart.last_packagescount, first_cart.packagescount) = 1 THEN '1 parcel'
            WHEN COALESCE(last_cart.last_packagescount, first_cart.packagescount) > 1 THEN '2 and more parcels'
        END AS last_parcels_num,
        COALESCE(last_cart.last_carttab, first_cart.carttab) AS last_carttab,
        last_cart.last_deliveryprice,
        last_cart.last_productsprice,
        last_cart.last_hasoptimalshippingtab,
        last_cart.last_optimalshippingdeliveryprice,
        -------------------------------------------------------------------------------------------- 
        max_status.max_parcels,
        max_status.max_items,
        max_status.max_cart,
        max_status.had_optimal
    FROM {{ ref('conversion_funnel') }} AS funnel
    LEFT JOIN cart_open AS first_cart
        ON
            funnel.device_id = first_cart.device_id
            AND funnel.cart_open_dt = first_cart.event_ts_cet
    LEFT JOIN sessions_last_cart AS last_cart
        ON
            funnel.device_id = last_cart.device_id
            AND funnel.session_minenv_dt = last_cart.session_minenv_dt
            AND last_cart.rnk = 1
    LEFT JOIN cart_switcher
        ON
            funnel.device_id = cart_switcher.device_id
            AND funnel.session_minenv_dt = cart_switcher.session_minenv_dt
    LEFT JOIN max_status
        ON
            funnel.device_id = max_status.device_id
            AND funnel.session_minenv_dt = max_status.session_minenv_dt
    WHERE
        funnel.window_size = '7 days'
        AND DATE(funnel.session_minenv_dt) >= '2023-06-01'
        AND funnel.cart_open_dt IS NOT NULL
),

two_parcel_scenarios AS (
    SELECT
        *,
        CASE
            WHEN funnel_cart.last_deliveryprice = 0 THEN 'free delivery'
            WHEN funnel_cart.last_deliveryprice > 0 AND funnel_cart.last_deliveryprice <= 5 THEN 'less then 5eur delivery'
            WHEN funnel_cart.last_deliveryprice > 5 THEN 'more then 5eur delivery'
        END AS delivery,
        GREATEST(funnel_cart.first_packagescount, funnel_cart.last_packagescount) AS packages_count,
        CASE
            WHEN
                funnel_cart.switcher_num = 0
                AND funnel_cart.max_parcels > 1
                AND funnel_cart.had_optimal
                AND funnel_cart.first_parcels_num = '1 parcel'
                AND funnel_cart.last_parcels_num = '1 parcel'
                THEN 'manually_optimised'
            WHEN
                funnel_cart.switcher_num = 0
                AND funnel_cart.first_carttab = 'optimalShipping'
                AND funnel_cart.last_carttab = 'optimalShipping'
                THEN 'optimal_pre_selected'
            WHEN
                funnel_cart.switcher_num = 0
                AND funnel_cart.max_parcels > 1
                AND NOT funnel_cart.had_optimal
                AND funnel_cart.first_carttab != 'optimalShipping'
                THEN 'no_optimal_shipping'
            WHEN
                funnel_cart.switcher_num = 0
                AND funnel_cart.max_parcels > 1
                AND funnel_cart.had_optimal
                AND funnel_cart.first_carttab != 'optimalShipping'
                THEN 'had_optimal_didnt_tried'
            WHEN
                funnel_cart.switcher_num > 0
                AND funnel_cart.first_carttab != 'optimalShipping'
                AND funnel_cart.last_carttab != 'optimalShipping'
                THEN 'tried_optimal_didnt_use'
            WHEN
                funnel_cart.switcher_num > 0
                AND funnel_cart.first_carttab != 'optimalShipping'
                AND funnel_cart.last_carttab = 'optimalShipping'
                THEN 'choose_optimal'
        END AS cart_scenarios
    FROM funnel_cart
)

SELECT *
FROM two_parcel_scenarios
