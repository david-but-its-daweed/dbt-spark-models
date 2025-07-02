{{
  config(
    materialized='incremental',
    alias='active_devices',
    file_format='parquet',
    incremental_strategy='insert_overwrite',
    partition_by=['month_msk'],
    meta = {
        'model_owner' : '@general_analytics',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'day',
        'bigquery_upload_horizon_days': '230',
        'priority_weight': '1000',
        'full_reload_on': '6',
    }
  )
}}


WITH device_info AS (
    SELECT
        ad.device_id,
        ad.date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(TO_DATE(ad.join_ts_msk)) AS join_dt,
        FIRST_VALUE(UPPER(ad.country)) AS country,
        FIRST_VALUE(LOWER(ad.os_type)) AS platform,
        FIRST_VALUE(ad.os_version) AS os_version,
        FIRST_VALUE(ad.app_version) AS app_version,
        MIN(ad.ephemeral) AS is_ephemeral,
        FIRST_VALUE(ad.real_user_id) AS real_user_id,
        FIRST_VALUE(IF(ad.legal_entity = 'jmt', 'JMT', 'SIA')) AS legal_entity,
        FIRST_VALUE(CASE WHEN ad.date_msk < '2023-07-01' THEN 'joom' ELSE ad.app_entity END) AS app_entity,
        FIRST_VALUE(CASE WHEN ad.date_msk < '2023-07-01' THEN 'joom' ELSE ad.app_entity_group END) AS app_entity_group,
        FIRST_VALUE(ad.custom_domain) AS custom_domain,
        FIRST_VALUE(COALESCE(f.productOpens > 0, FALSE)) AS is_product_opened,
        FIRST_VALUE(COALESCE(f.productAddToCarts > 0, FALSE)) AS is_product_added_to_cart,
        FIRST_VALUE(COALESCE(f.productPurchases > 0, FALSE)) AS is_product_purchased,
        FIRST_VALUE(COALESCE(f.productToFavourites > 0, FALSE)) AS is_product_to_favourites,
        FIRST_VALUE(COALESCE(f.cartOpens > 0, FALSE)) AS is_cart_opened,
        FIRST_VALUE(COALESCE(f.checkoutStarts > 0, FALSE)) AS is_checkout_started,
        FIRST_VALUE(COALESCE(f.checkoutPaymentMethodSelects > 0, FALSE)) AS is_checkout_payment_method_selected,
        FIRST_VALUE(COALESCE(f.checkoutDeliverySelects > 0, FALSE)) AS is_checkout_delivery_selected,
        FIRST_VALUE(UPPER(ad.language)) AS app_language
    FROM {{ source('mart', 'star_active_device') }} AS ad
    LEFT JOIN {{ ref('active_devices_funnel') }} AS f USING (device_id, date_msk)
    {% if is_incremental() %}
        WHERE ad.date_msk >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
    {% endif %}
    GROUP BY 1, 2
),

min_dates AS (
    SELECT
        device_id,
        MIN(date_msk) AS dt
    FROM {{ source('mart', 'star_active_device') }}
    GROUP BY 1
)

SELECT
    d.device_id,
    d.day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
    IF(
        min_dates.dt < d.join_dt,
        min_dates.dt,
        d.join_dt
    ) AS join_day,
    d.country,
    d.platform,
    d.os_version,
    d.app_version,
    d.app_entity,
    d.app_entity_group,
    CASE
        WHEN UPPER(d.app_entity) = 'SHOPY' THEN d.custom_domain
    END AS shopy_blogger_domain,
    d.is_product_opened,
    d.is_product_added_to_cart,
    d.is_product_purchased,
    d.is_product_to_favourites,
    d.is_cart_opened,
    d.is_checkout_started,
    d.is_checkout_payment_method_selected,
    d.is_checkout_delivery_selected,
    d.is_ephemeral,
    d.day = join_day AS is_new_user,
    d.real_user_id,
    d.legal_entity,
    d.app_language,
    TRUNC(d.day, 'MM') AS month_msk
FROM device_info AS d
INNER JOIN min_dates USING (device_id)
DISTRIBUTE BY month_msk, ABS(HASH(d.device_id)) % 10
