{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@marksysoev',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH business_hours AS (
    SELECT
        all_hours.date_hour_cet,
        ROW_NUMBER() OVER (ORDER BY all_hours.date_hour_cet) AS row_number
    FROM
        {{ ref("onfy_support_business_hours") }} AS all_hours
),

ticket_create_prev_hour AS (
    SELECT
        mart.onfy_babylone_events.payload.ticketid AS ticket_id,
        FROM_UTC_TIMESTAMP(mart.onfy_babylone_events.event_ts_utc, 'Europe/Berlin') AS event_ts_cet,
        mart.onfy_babylone_events.payload.messagesource,
        MAX(prev_business_hour.date_hour_cet) AS prev_business_hour,
        MAX_BY(prev_business_hour.row_number, prev_business_hour.date_hour_cet) AS prev_business_hour_row_number
    FROM
        {{ source('mart', 'onfy_babylone_events') }}
    LEFT JOIN business_hours AS prev_business_hour
        ON prev_business_hour.date_hour_cet <= FROM_UTC_TIMESTAMP(mart.onfy_babylone_events.event_ts_utc, 'Europe/Berlin')
    WHERE
        mart.onfy_babylone_events.type = 'ticketCreate'
    GROUP BY
        mart.onfy_babylone_events.id,
        mart.onfy_babylone_events.payload.ticketid,
        mart.onfy_babylone_events.event_ts_utc,
        mart.onfy_babylone_events.payload.messagesource
),

ticket_create_next_hour AS (
    SELECT DISTINCT
        ticket_create_prev_hour.ticket_id,
        ticket_create_prev_hour.event_ts_cet,
        ticket_create_prev_hour.messagesource,
        ticket_create_prev_hour.prev_business_hour,
        ticket_create_prev_hour.prev_business_hour_row_number,
        next_hour.date_hour_cet AS next_business_hour
    FROM
        ticket_create_prev_hour
    LEFT JOIN business_hours AS next_hour
        ON next_hour.row_number = ticket_create_prev_hour.prev_business_hour_row_number + 1
),

ticket_create_predata AS (
    SELECT DISTINCT
        ticket_create_next_hour.ticket_id,
        ticket_create_next_hour.event_ts_cet,
        ticket_create_next_hour.messagesource,
        ticket_create_next_hour.prev_business_hour,
        ticket_create_next_hour.next_business_hour,
        CASE
            WHEN DATE_TRUNC('HOUR', ticket_create_next_hour.event_ts_cet) = ticket_create_next_hour.prev_business_hour
                THEN TO_TIMESTAMP(UNIX_SECONDS(sla_8.date_hour_cet) + UNIX_SECONDS(ticket_create_next_hour.event_ts_cet) - UNIX_SECONDS(ticket_create_next_hour.prev_business_hour))
            ELSE sla_8.date_hour_cet + INTERVAL 1 HOUR
        END AS sla_8,
        CASE
            WHEN DATE_TRUNC('HOUR', ticket_create_next_hour.event_ts_cet) = ticket_create_next_hour.prev_business_hour
                THEN TO_TIMESTAMP(UNIX_SECONDS(sla_22.date_hour_cet) + UNIX_SECONDS(ticket_create_next_hour.event_ts_cet) - UNIX_SECONDS(ticket_create_next_hour.prev_business_hour))
            ELSE sla_22.date_hour_cet + INTERVAL 1 HOUR
        END AS sla_22
    FROM
        ticket_create_next_hour
    LEFT JOIN business_hours AS sla_8
        ON ticket_create_next_hour.prev_business_hour_row_number + 8 = sla_8.row_number
    LEFT JOIN business_hours AS sla_22
        ON ticket_create_next_hour.prev_business_hour_row_number + 22 = sla_22.row_number
),

last_tag_ids AS (
    SELECT
        onfy_babylone_events.payload.ticketid AS ticket_id,
        FROM_UTC_TIMESTAMP(MAX(onfy_babylone_events.event_ts_utc), 'Europe/Berlin') AS event_ts_cet,
        MAX_BY(onfy_babylone_events.payload.tagids, onfy_babylone_events.event_ts_utc) AS tag_ids
    FROM
        {{ source('mart', 'onfy_babylone_events') }}
    WHERE
        onfy_babylone_events.type = 'ticketChange'
        AND ARRAY_SIZE(onfy_babylone_events.payload.tagids) > 0
    GROUP BY
        onfy_babylone_events.payload.ticketid
),

tag_ids AS (
    SELECT DISTINCT
        last_tag_ids.ticket_id,
        last_tag_ids.event_ts_cet,
        EXPLODE(last_tag_ids.tag_ids) AS tag_id
    FROM
        last_tag_ids
),

tag_names AS (
    SELECT DISTINCT
        tag_ids.*,
        babylone_onfy_tags_daily_snapshot.name AS tag_name
    FROM
        tag_ids
    LEFT JOIN {{ source('mongo', 'babylone_onfy_tags_daily_snapshot') }}
        ON tag_ids.tag_id = babylone_onfy_tags_daily_snapshot._id
),

ticket_entry AS (
    SELECT DISTINCT
        onfy_babylone_events.id,
        onfy_babylone_events.payload.ticketid AS ticket_id,
        FROM_UTC_TIMESTAMP(onfy_babylone_events.event_ts_utc, 'Europe/Berlin') AS event_ts_cet,
        --,onfy_babylone_events.payload.changedById
        --,onfy_babylone_events.payload.stateOwner
        onfy_babylone_events.payload.authortype
    FROM
        {{ source('mart', 'onfy_babylone_events') }}
    WHERE
        onfy_babylone_events.type = 'ticketEntryAdd'
        AND onfy_babylone_events.payload.entrytype = 'message'
),

ticket_resolved AS (
    SELECT DISTINCT
        onfy_babylone_events.id,
        onfy_babylone_events.payload.ticketid AS ticket_id,
        FROM_UTC_TIMESTAMP(onfy_babylone_events.event_ts_utc, 'Europe/Berlin') AS event_ts_cet,
        onfy_babylone_events.payload.stateowner
    FROM
        {{ source('mart', 'onfy_babylone_events') }}
    WHERE
        onfy_babylone_events.type = 'ticketChange'
        AND onfy_babylone_events.payload.stateowner IN ('Resolved', 'Rejected')
),

ticket_create AS (
    SELECT
        ticket_create_predata.ticket_id,
        ticket_create_predata.event_ts_cet,
        ticket_create_predata.messagesource,
        ticket_create_predata.sla_8,
        ticket_create_predata.sla_22,
        MIN(customer_message.event_ts_cet) AS first_customer_message_cet,
        MIN(agent_message.event_ts_cet) AS first_agent_message_cet,
        MIN(ticket_resolved.event_ts_cet) AS ticket_resolved_cet,
        MIN_BY(ticket_resolved.stateowner, ticket_resolved.event_ts_cet) AS resolution
    FROM
        ticket_create_predata
    LEFT JOIN ticket_entry AS customer_message
        ON
            customer_message.ticket_id = ticket_create_predata.ticket_id
            AND customer_message.authortype = 'customer'
    LEFT JOIN ticket_entry AS agent_message
        ON
            agent_message.ticket_id = ticket_create_predata.ticket_id
            AND agent_message.authortype = 'agent'
    LEFT JOIN ticket_resolved
        ON ticket_resolved.ticket_id = ticket_create_predata.ticket_id
    GROUP BY
        ticket_create_predata.ticket_id,
        ticket_create_predata.event_ts_cet,
        ticket_create_predata.messagesource,
        ticket_create_predata.sla_8,
        ticket_create_predata.sla_22
)

SELECT DISTINCT
    ticket_create.ticket_id,
    ticket_create.event_ts_cet AS ticket_created_cet,
    ticket_create.messagesource,
    ticket_create.first_customer_message_cet,
    ticket_create.first_agent_message_cet,
    ticket_create.ticket_resolved_cet,
    ticket_create.sla_8,
    ticket_create.sla_22,
    CASE WHEN ticket_create.first_agent_message_cet < ticket_create.sla_8 THEN 1 ELSE 0 END AS agent_reply_sla,
    CASE WHEN ticket_create.ticket_resolved_cet < ticket_create.sla_22 THEN 1 ELSE 0 END AS resolution_sla,
    /*
    (
        UNIX_SECONDS(TIMESTAMP(ticket_create.first_agent_message_cet))
        - UNIX_SECONDS(TIMESTAMP(ticket_created_cet))
    ) / 3600 AS time_to_first_reply,
    (
        UNIX_SECONDS(TIMESTAMP(ticket_create.ticket_resolved_cet))
        - UNIX_SECONDS(TIMESTAMP(ticket_created_cet))
    ) / 3600 AS time_to_resolution,
    */
    ticket_create.resolution,
    CASE WHEN tag_customer.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_customer,
    CASE WHEN tag_pharmacy.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_pharmacy,
    CASE WHEN tag_complaint_expiry_date.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_complaint_expiry_date,
    CASE WHEN tag_complaint_wrong_item.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_complaint_wrong_item,
    CASE WHEN tag_complaint_payment_not_received.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_complaint_payment_not_received,
    CASE WHEN tag_complaint_damage.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_complaint_damage,
    CASE WHEN tag_complaint_pharmaceutical_reasons.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_complaint_pharmaceutical_reasons,
    CASE WHEN tag_complaint_item_missing.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_complaint_item_missing,
    CASE WHEN tag_delivery_abroad.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_delivery_abroad,
    CASE WHEN tag_delivery_late.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_delivery_late,
    CASE WHEN tag_delivery_inquiry.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_delivery_inquiry,
    CASE WHEN tag_delivery_lost_orders.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_delivery_lost_orders,
    CASE WHEN tag_delivery_undelivered.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_delivery_undelivered,
    CASE WHEN tag_delivery_wrong_info.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_delivery_wrong_info,
    CASE WHEN tag_feedback_bug.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_feedback_bug,
    CASE WHEN tag_feedback_improvemenet.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_feedback_improvemenet,
    CASE WHEN tag_feedback_gdpr.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_feedback_gdpr,
    CASE WHEN tag_feedback_unsubscribe.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_feedback_unsubscribe,
    CASE WHEN tag_general_requests.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_general_requests,
    CASE WHEN tag_general_requrest_payment.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_general_requrest_payment,
    CASE WHEN tag_pre_sales_pharmaceutical_reasons.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_pre_sales_pharmaceutical_reasons,
    CASE WHEN tag_general_requrest_promo_code.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_general_requrest_promo_code,
    CASE WHEN tag_internal.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_internal,
    CASE WHEN tag_order_confirmation.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_order_confirmation,
    CASE WHEN tag_order_change.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_order_change,
    CASE WHEN tag_order_unwanted.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_order_unwanted,
    CASE WHEN tag_order_not_in_stock.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_order_not_in_stock,
    CASE WHEN tag_order_limit.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_order_limit,
    CASE WHEN tag_order_invoice.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_order_invoice,
    CASE WHEN tag_payment_paypal.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_payment_paypal,
    CASE WHEN tag_other.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_other,
    CASE WHEN tag_e_rx.ticket_id IS NOT NULL THEN 1 ELSE 0 END AS tag_e_rx

FROM
    ticket_create
LEFT JOIN ticket_entry AS customer_message
    ON
        customer_message.ticket_id = ticket_create.ticket_id
        AND customer_message.authortype = 'customer'
LEFT JOIN ticket_entry AS agent_message
    ON
        agent_message.ticket_id = ticket_create.ticket_id
        AND agent_message.authortype = 'agent'
LEFT JOIN ticket_resolved
    ON ticket_resolved.ticket_id = ticket_create.ticket_id
LEFT JOIN tag_names AS tag_customer
    ON
        tag_customer.ticket_id = ticket_create.ticket_id
        AND tag_customer.tag_name = 'customer'
LEFT JOIN tag_names AS tag_pharmacy
    ON
        tag_pharmacy.ticket_id = ticket_create.ticket_id
        AND tag_pharmacy.tag_name = 'pharmacy'
LEFT JOIN tag_names AS tag_complaint_expiry_date
    ON
        tag_complaint_expiry_date.ticket_id = ticket_create.ticket_id
        AND tag_complaint_expiry_date.tag_name = 'complaint_expiry_date'
LEFT JOIN tag_names AS tag_complaint_wrong_item
    ON
        tag_complaint_wrong_item.ticket_id = ticket_create.ticket_id
        AND tag_complaint_wrong_item.tag_name = 'complaint_wrong_item'
LEFT JOIN tag_names AS tag_complaint_payment_not_received
    ON
        tag_complaint_payment_not_received.ticket_id = ticket_create.ticket_id
        AND tag_complaint_payment_not_received.tag_name = 'complaint_payment_not_received'
LEFT JOIN tag_names AS tag_complaint_damage
    ON
        tag_complaint_damage.ticket_id = ticket_create.ticket_id
        AND tag_complaint_damage.tag_name = 'complaint_damage'
LEFT JOIN tag_names AS tag_complaint_pharmaceutical_reasons
    ON
        tag_complaint_pharmaceutical_reasons.ticket_id = ticket_create.ticket_id
        AND tag_complaint_pharmaceutical_reasons.tag_name = 'complaint_pharmaceutical_reasons'
LEFT JOIN tag_names AS tag_complaint_item_missing
    ON
        tag_complaint_item_missing.ticket_id = ticket_create.ticket_id
        AND tag_complaint_item_missing.tag_name = 'complaint_item_missing'
LEFT JOIN tag_names AS tag_delivery_abroad
    ON
        tag_delivery_abroad.ticket_id = ticket_create.ticket_id
        AND tag_delivery_abroad.tag_name = 'delivery_abroad'
LEFT JOIN tag_names AS tag_delivery_late
    ON
        tag_delivery_late.ticket_id = ticket_create.ticket_id
        AND tag_delivery_late.tag_name = 'delivery_late'
LEFT JOIN tag_names AS tag_delivery_inquiry
    ON
        tag_delivery_inquiry.ticket_id = ticket_create.ticket_id
        AND tag_delivery_inquiry.tag_name = 'delivery_inquiry'
LEFT JOIN tag_names AS tag_delivery_lost_orders
    ON
        tag_delivery_lost_orders.ticket_id = ticket_create.ticket_id
        AND tag_delivery_lost_orders.tag_name = 'delivery_lost_orders'
LEFT JOIN tag_names AS tag_delivery_undelivered
    ON
        tag_delivery_undelivered.ticket_id = ticket_create.ticket_id
        AND tag_delivery_undelivered.tag_name = 'delivery_undelivered'
LEFT JOIN tag_names AS tag_delivery_wrong_info
    ON
        tag_delivery_wrong_info.ticket_id = ticket_create.ticket_id
        AND tag_delivery_wrong_info.tag_name = 'delivery_wrong_info'
LEFT JOIN tag_names AS tag_feedback_bug
    ON
        tag_feedback_bug.ticket_id = ticket_create.ticket_id
        AND tag_feedback_bug.tag_name = 'feedback_bug'
LEFT JOIN tag_names AS tag_feedback_improvemenet
    ON
        tag_feedback_improvemenet.ticket_id = ticket_create.ticket_id
        AND tag_feedback_improvemenet.tag_name = 'feedback_improvemenet'
LEFT JOIN tag_names AS tag_feedback_gdpr
    ON
        tag_feedback_gdpr.ticket_id = ticket_create.ticket_id
        AND tag_feedback_gdpr.tag_name = 'feedback_gdpr'
LEFT JOIN tag_names AS tag_feedback_unsubscribe
    ON
        tag_feedback_unsubscribe.ticket_id = ticket_create.ticket_id
        AND tag_feedback_unsubscribe.tag_name = 'feedback_unsubscribe'
LEFT JOIN tag_names AS tag_general_requests
    ON
        tag_general_requests.ticket_id = ticket_create.ticket_id
        AND tag_general_requests.tag_name = 'general_requests'
LEFT JOIN tag_names AS tag_general_requrest_payment
    ON
        tag_general_requrest_payment.ticket_id = ticket_create.ticket_id
        AND tag_general_requrest_payment.tag_name = 'general_requrest_payment'
LEFT JOIN tag_names AS tag_pre_sales_pharmaceutical_reasons
    ON
        tag_pre_sales_pharmaceutical_reasons.ticket_id = ticket_create.ticket_id
        AND tag_pre_sales_pharmaceutical_reasons.tag_name = 'pre_sales_pharmaceutical_reasons'
LEFT JOIN tag_names AS tag_general_requrest_promo_code
    ON
        tag_general_requrest_promo_code.ticket_id = ticket_create.ticket_id
        AND tag_general_requrest_promo_code.tag_name = 'general_requrest_promo_code'
LEFT JOIN tag_names AS tag_internal
    ON
        tag_internal.ticket_id = ticket_create.ticket_id
        AND tag_internal.tag_name = 'internal'
LEFT JOIN tag_names AS tag_order_confirmation
    ON
        tag_order_confirmation.ticket_id = ticket_create.ticket_id
        AND tag_order_confirmation.tag_name = 'order_confirmation'
LEFT JOIN tag_names AS tag_order_change
    ON
        tag_order_change.ticket_id = ticket_create.ticket_id
        AND tag_order_change.tag_name = 'order_change'
LEFT JOIN tag_names AS tag_order_unwanted
    ON
        tag_order_unwanted.ticket_id = ticket_create.ticket_id
        AND tag_order_unwanted.tag_name = 'order_unwanted'
LEFT JOIN tag_names AS tag_order_not_in_stock
    ON
        tag_order_not_in_stock.ticket_id = ticket_create.ticket_id
        AND tag_order_not_in_stock.tag_name = 'order_not_in_stock'
LEFT JOIN tag_names AS tag_order_limit
    ON
        tag_order_limit.ticket_id = ticket_create.ticket_id
        AND tag_order_limit.tag_name = 'order_limit'
LEFT JOIN tag_names AS tag_order_invoice
    ON
        tag_order_invoice.ticket_id = ticket_create.ticket_id
        AND tag_order_invoice.tag_name = 'order_invoice'
LEFT JOIN tag_names AS tag_payment_paypal
    ON
        tag_payment_paypal.ticket_id = ticket_create.ticket_id
        AND tag_payment_paypal.tag_name = 'payment_paypal'
LEFT JOIN tag_names AS tag_other
    ON
        tag_other.ticket_id = ticket_create.ticket_id
        AND tag_other.tag_name = 'Other'
LEFT JOIN tag_names AS tag_e_rx
    ON
        tag_e_rx.ticket_id = ticket_create.ticket_id
        AND tag_e_rx.tag_name = 'e-Rx'
