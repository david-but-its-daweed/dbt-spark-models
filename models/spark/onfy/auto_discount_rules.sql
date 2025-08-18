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
    
WITH price_history AS (
    SELECT 
        date_value,
        effective_ts,
        product_id,
        pzn,
        manufacturer_name,
        MIN(price) AS min_price
    FROM (
        SELECT EXPLODE(
            SEQUENCE(
                DATE_SUB(CURRENT_DATE(), 3),  
                CURRENT_DATE(),               
                INTERVAL 1 DAY             
            )
        ) AS date_value
    ) d
    LEFT JOIN {{source('onfy_mart', 'dim_product')}}
        ON d.date_value BETWEEN dim_product.effective_ts AND dim_product.next_effective_ts
    WHERE store_state = 'DEFAULT'
        AND next_effective_ts >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY
    GROUP BY 
        effective_ts, 
        product_id, 
        pzn, 
        manufacturer_name, 
        date_value
        
),

min_per_date AS
(
    SELECT
        date_value,
        product_id,
        pzn,
        manufacturer_name,
        MIN(min_price) AS min_price
    FROM price_history
    WHERE 1=1
        AND date_value >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY
    GROUP BY 
        date_value,
        product_id,
        pzn,
        manufacturer_name
),

price_change AS
(
    SELECT
        date_value,
        product_id,
        pzn,
        min_price,
        IF(LAG(min_price) OVER (PARTITION BY product_id ORDER BY date_value) > min_price, 1, 0) AS price_change
    FROM min_per_date
    WHERE 1=1
),

pzn_dict AS
(
    SELECT DISTINCT 
        product_id,
        pzn
    FROM price_history
),

latest_price_changes AS 
(
    SELECT 
        product_id,
        pzn,
        MAX(price_change) AS latest_price_decrease
    FROM price_change
    GROUP BY
        product_id,
        pzn
),

sessions AS 
(
    SELECT 
        source AS utm_source,
        attributed_landing_pzn AS product_id,
        device_id,
        attributed_session_dt AS session_ts_cet,
        CASE 
            WHEN source = 'google' AND (campaign LIKE 'shopping%' OR campaign LIKE 'ohm%') THEN 'google'
            WHEN source = 'idealo' THEN 'idealo'
            WHEN source = 'billiger' THEN 'billiger'
            WHEN source = 'medizinfuchs' THEN 'medizinfuchs'
        END AS source,
        order_id,
        order_dt,
        promocode_discount,
        gross_profit_initial,
        gmv_initial,
        session_spend
    FROM {{ ref('ads_dashboard') }}
    WHERE 1=1
        AND session_dt >= CURRENT_DATE() - INTERVAL 1 MONTH
        AND ((source = 'google' AND (campaign LIKE 'shopping%' OR campaign LIKE 'ohm%')) 
            OR source IN ('idealo', 'billiger', 'medizinfuchs'))
        AND attributed_landing_pzn IS NOT NULL
        AND attributed_landing_pzn <> ''
),

calculations AS (
    SELECT
        source,
        pzn_dict.product_id as product_id,
        pzn_dict.pzn,
        COUNT(DISTINCT CONCAT(device_id, session_ts_cet)) AS sessions,
        COUNT(DISTINCT order_id) AS orders,
        latest_price_decrease,
        SUM(CAST(promocode_discount AS FLOAT)) AS promocode_discount,
        SUM(gmv_initial) AS gmv_initial,
        SUM(gross_profit_initial) AS gross_profit_initial,
        CASE source 
            WHEN 'billiger' THEN SUM(gross_profit_initial + COALESCE(promocode_discount, 0)) / 
                (SUM(session_spend) + SUM(COALESCE(promocode_discount, 0)))
            WHEN 'idealo' THEN SUM(gross_profit_initial + COALESCE(promocode_discount, 0)) / 
                (SUM(session_spend) + SUM(COALESCE(promocode_discount, 0)))
            ELSE 1
        END AS roas,
        COUNT(DISTINCT order_id) / COUNT(DISTINCT CONCAT(device_id, session_ts_cet)) AS cr
    FROM sessions
    LEFT JOIN pzn_dict
        ON sessions.product_id = pzn_dict.product_id
        OR sessions.product_id = pzn_dict.pzn
    LEFT JOIN latest_price_changes
        ON latest_price_changes.product_id = sessions.product_id
    GROUP BY 
        source,
        sessions.product_id,
        pzn_dict.product_id,
        pzn_dict.pzn,
        latest_price_decrease
)



SELECT 
    product_id,
    blacklist_rules.channel,
    31 AS weight,
    blacklist_rules.pessimization_type,
    0.0 AS discount_percent,
    blacklist_rules.source,
    blacklist_rules.comment
FROM (
    SELECT 
        product_id,
        source AS channel,
        'use_max_price' AS pessimization_type,
        'low_performing' AS source,
        'Billiger low performing blacklist' AS comment,
        roas,
        cr,
        sessions,
        CEILING(AVG(sessions) OVER ()) AS average
    FROM calculations 
    WHERE source = 'billiger'
        AND (cr <= 0.001 OR roas <= 0.3)
        AND latest_price_decrease <> 1
    
    UNION ALL
    
    SELECT 
        product_id,
        source AS channel,
        'use_max_price' AS pessimization_type,
        'low_performing' AS source,
        'Idealo low performing blacklist' AS comment,
        roas,
        cr,
        sessions,
        CEILING(AVG(sessions) OVER ()) AS average
    FROM calculations
    WHERE source = 'idealo'
        AND (cr <= 0.001 OR roas <= 0.5)
        AND latest_price_decrease <> 1
    
    UNION ALL
    
    SELECT 
        product_id,
        source AS channel,
        'use_max_price' AS pessimization_type,
        'low_performing' AS source,
        'Medizinfuchs low performing blacklist' AS comment,
        roas,
        cr,
        sessions,
        CEILING(AVG(sessions) OVER ()) AS average
    FROM calculations 
    WHERE source = 'medizinfuchs'
        AND (cr <= 0.001 OR roas <= 0.3)
        AND latest_price_decrease <> 1
    
    UNION ALL
    
    -- Google Shopping blacklist (both google and ohm channels)
    SELECT 
        product_id,
        'google' as channel,
        'filter_out' AS pessimization_type,
        'blacklist' AS source,
        'Google Shopping low performing blacklist' AS comment,
        roas,
        cr,
        sessions,
        CEILING(AVG(sessions) OVER ()) AS average
    FROM calculations
    WHERE source = 'google'
        AND cr <= 0.001
    
    UNION ALL
    
    SELECT 
        product_id,
        'ohm' as channel,
        'filter_out' AS pessimization_type,
        'blacklist' AS source,
        'OHM low performing blacklist' AS comment,
        roas,
        cr,
        sessions,
        CEILING(AVG(sessions) OVER ()) AS average
    FROM calculations
    WHERE source = 'google'
        AND cr <= 0.001  
) blacklist_rules
WHERE 1=1
    AND sessions > average
