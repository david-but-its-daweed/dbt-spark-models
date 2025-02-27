{{
    config(
        materialized='table',
        meta = {
            'model_owner' : '@general_analytics',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_check_counts_max_diff_fraction': '0.01'
        }
    )
}}


SELECT
    adtech.partition_date AS date_msk,
    adtech.device_id,
    adtech.user_id,
    link.real_user_id,
    COALESCE(active_devices.country_code, UPPER(dim_device.pref_country), UPPER(adtech.country)) AS country_code,
    COALESCE(countries.country_priority_type, 'Other') AS country_priority_type,
    COALESCE(active_devices.platform, dim_device.os_type, adtech.os_type) AS platform,
    COALESCE(
        active_devices.app_entity,
        CASE
            WHEN dim_device.app_entity = 'joom'
                THEN 'Joom'
            WHEN dim_device.app_entity = 'joom_geek'
                THEN 'Joom Geek'
            WHEN dim_device.app_entity = 'cool_be'
                THEN 'CoolBe'
            WHEN dim_device.app_entity = 'cool_be_com'
                THEN 'CoolBeCom'
            WHEN dim_device.app_entity = 'cool_be_trending'
                THEN 'CoolBe Trending'
        END
    ) AS app_entity,
    SUM(adtech.adtech_revenue) AS adtech_revenue
FROM {{ source('mart', 'adtech_revenues') }} AS adtech
LEFT JOIN {{ source('mart', 'dim_device') }} AS dim_device -- используем в случае, если по gold не находится пара (из-за стыка дат)
    ON
        adtech.device_id = dim_device.device_id
        AND dim_device.next_effective_ts = TIMESTAMP '9999-12-31 23:59:59 UTC'
LEFT JOIN {{ ref('gold_active_devices') }} AS active_devices -- используем с первым периоритетом, там самая корректная логика для страны сейчас
    ON
        active_devices.device_id = adtech.device_id
        AND active_devices.date_msk = adtech.partition_date
LEFT JOIN {{ source('mart', 'link_device_real_user') }} AS link USING (device_id) -- берем real_user_id, из gold не берем также из-за проблемы стыка дат
LEFT JOIN {{ ref('gold_countries') }} AS countries
    ON
        COALESCE(active_devices.country_code, UPPER(dim_device.pref_country), UPPER(adtech.country)) = countries.country_code
WHERE adtech.partition_date >= '2022-01-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
