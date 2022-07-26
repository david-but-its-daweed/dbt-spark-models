{{ config(
    schema='customer_routing',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}
SELECT
    device_id AS device_id,
    date(event_ts_msk) AS partition_date_msk,
    count(
        if(type IN ('productOpen', 'productToCart',
            'productLike', 'productPurchase', 'orderParcelOpen',
            'orderParcelPreview', 'ordersOpen', 'notificationCenterOpen',
            'notificationPreview'),
            device_id, NULL)) > 0 AS has_non_empty_activity,
    count(
        if(type IN ('productOpen', 'productToCart',
            'productLike',
            'productPurchase'), device_id, NULL)) > 0 AS has_funnel_activity,
    count(
        if(type IN ('orderParcelOpen', 'orderParcelPreview',
            'ordersOpen'), device_id, NULL)) > 0 AS has_orders_check_activity,
    count(
        if(type IN ('notificationCenterOpen', 'notificationPreview'),
            device_id, NULL)) > 0 AS has_notification_activity,
    count(
        if(type IN ('productToCart', 'productToFavorites',
            'productToCollection'),
            device_id, NULL)) > 0 AS has_meaningful_activity
FROM {{ source('mart', 'device_events') }}
WHERE
    `type` IN ('sessionConfigured',
        'productOpen', 'productToCart',
        'productLike', 'productPurchase',
        'productToFavorites', 'productToCollection',
        'orderParcelOpen', 'orderParcelPreview',
        'ordersOpen',
        'notificationCenterOpen', 'notificationPreview')
    {% if is_incremental() %}
        AND date(event_ts_msk) >= date'{{ var("start_date_ymd") }}'
        AND date(event_ts_msk) < date'{{ var("end_date_ymd") }}'
    {% else %}
        AND date(event_ts_msk) >= date'2022-06-01'
    {% endif %}
GROUP BY 1, 2
ORDER BY 2 ASC
