{{ config(
    schema='onfy',
    materialized='view',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}


WITH order_parcels AS 
(
    SELECT DISTINCT 
        order_parcel.id AS order_parcel_id,
        order_parcel.tracking_number,
        order_parcel.shipping_method,
        store.name AS store_name,
        from_utc_timestamp(order.created, 'Europe/Berlin') AS order_created_datetime_cet,

        CASE 
            WHEN 
                WEEKDAY(from_utc_timestamp(order.created, 'Europe/Berlin')) IN (4, 5, 6) -- пт-вск
            THEN 
                DATE_TRUNC('WEEK', from_utc_timestamp(order.created, 'Europe/Berlin')) + INTERVAL 1 WEEK 12 HOURS -- полдень следующего понедельника
            ELSE -- если пт-сб
                CAST(from_utc_timestamp(order.created, 'Europe/Berlin') AS DATE) + INTERVAL 1 DAY 12 HOURS -- полдень следующего дня
        END AS planned_transfer_datetime_cet,

        CASE 
            WHEN 
                WEEKDAY(from_utc_timestamp(order.created, 'Europe/Berlin')) IN (4, 5, 6) -- пт-вск
            THEN 
                DATE_TRUNC('WEEK', from_utc_timestamp(order.created, 'Europe/Berlin')) + INTERVAL 1 WEEK 1 DAY 12 HOURS -- полдень понедельника след недели
            WHEN 
                WEEKDAY(from_utc_timestamp(order.created, 'Europe/Berlin')) = 3 -- чт
            THEN  
                DATE_TRUNC('WEEK', from_utc_timestamp(order.created, 'Europe/Berlin')) + INTERVAL 1 WEEK  12 HOURS
            ELSE -- пн-ср
                CAST(from_utc_timestamp(order.created, 'Europe/Berlin') AS DATE) + INTERVAL 2 DAY 12 HOURS -- полдень через 1 рабочий день
        END AS one_day_after_planned_transfer_cet,

        CASE 
            WHEN 
                WEEKDAY(from_utc_timestamp(order.created, 'Europe/Berlin')) IN (4, 5, 6)
            THEN 
                DATE_TRUNC('WEEK', from_utc_timestamp(order.created, 'Europe/Berlin')) + INTERVAL 1 WEEK 3 DAYS 12 HOURS
            WHEN 
                WEEKDAY(from_utc_timestamp(order.created, 'Europe/Berlin')) = 0 -- пн
            THEN 
                CAST(from_utc_timestamp(order.created, 'Europe/Berlin') AS DATE) + INTERVAL 4 DAY 12 HOURS
            ELSE -- вт-чт
                CAST(from_utc_timestamp(order.created, 'Europe/Berlin') AS DATE) + INTERVAL 6 DAY 12 HOURS -- полдень через 3 рабочих дня после плановой передачи заказа
        END AS three_days_after_planned_transfer_cet,

        CASE 
            WHEN 
                WEEKDAY(from_utc_timestamp(order.created, 'Europe/Berlin')) IN (4, 5, 6) -- пт-сб
            THEN
                DATE_TRUNC('WEEK', from_utc_timestamp(order.created, 'Europe/Berlin')) + INTERVAL 2 WEEK 12 HOURS --понедельник через неделю
            ELSE 
                CAST(from_utc_timestamp(order.created, 'Europe/Berlin') AS DATE) + INTERVAL 8 DAY 12 HOURS -- полдень через 3 рабочих дня после плановой передачи заказа
        END AS five_days_after_planned_transfer_cet

    FROM
        {{ source('pharmacy_landing', 'order_parcel') }} as order_parcel
        LEFT JOIN {{ source('pharmacy_landing', 'order') }} as order
            ON order_parcel.order_id = order.id
        LEFT JOIN {{ source('pharmacy_landing', 'store') }} as store
            ON store.id = order_parcel.store_id
)

SELECT
    order_parcels.order_parcel_id,
    order_parcels.tracking_number,
    order_parcels.shipping_method,
    order_parcels.store_name,
    order_parcels.order_created_datetime_cet,
    order_parcels.planned_transfer_datetime_cet,
    order_parcels.one_day_after_planned_transfer_cet,
    order_parcels.three_days_after_planned_transfer_cet,
    order_parcels.five_days_after_planned_transfer_cet,
    CASE WHEN order_parcels.planned_transfer_datetime_cet < from_utc_timestamp(CURRENT_TIMESTAMP, 'Europe/Berlin') THEN TRUE ELSE FALSE END AS transfer_flag,
    CASE WHEN order_parcels.one_day_after_planned_transfer_cet < from_utc_timestamp(CURRENT_TIMESTAMP, 'Europe/Berlin') THEN TRUE ELSE FALSE END AS one_day_flag,
    CASE WHEN order_parcels.three_days_after_planned_transfer_cet < from_utc_timestamp(CURRENT_TIMESTAMP, 'Europe/Berlin') THEN TRUE ELSE FALSE END AS three_days_flag,
    CASE WHEN order_parcels.five_days_after_planned_transfer_cet < from_utc_timestamp(CURRENT_TIMESTAMP, 'Europe/Berlin') THEN TRUE ELSE FALSE END AS five_days_flag,
    from_utc_timestamp(MIN(status_info_received.checkpoint_time), 'Europe/Berlin') as info_received_datetime_cet,
    from_utc_timestamp(MIN(status_in_transit.checkpoint_time), 'Europe/Berlin') as in_transit_datetime_cet,
    from_utc_timestamp(MIN(status_delivered.checkpoint_time), 'Europe/Berlin') as delivered_datetime_cet
FROM 
    order_parcels
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_checkpoint') }} AS status_info_received
        ON order_parcels.order_parcel_id = status_info_received.order_parcel_id
        AND status_info_received.status = 'INFO_RECEIVED'
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_checkpoint') }} AS status_in_transit
        ON order_parcels.order_parcel_id = status_in_transit.order_parcel_id
        AND status_in_transit.status = 'IN_TRANSIT'  
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_checkpoint') }} AS status_delivered
        ON order_parcels.order_parcel_id = status_delivered.order_parcel_id
        AND status_delivered.status in ('DELIVERED', 'AVAILABLE_FOR_PICKUP')
GROUP BY
    order_parcels.order_parcel_id,
    order_parcels.tracking_number,
    order_parcels.shipping_method,
    order_parcels.store_name,
    order_parcels.order_created_datetime_cet,
    order_parcels.planned_transfer_datetime_cet,
    order_parcels.one_day_after_planned_transfer_cet,
    order_parcels.three_days_after_planned_transfer_cet,
    order_parcels.five_days_after_planned_transfer_cet 
