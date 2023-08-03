{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH orders AS (
    SELECT
        o.order_id,
        o.created_ts_msk,
        o.friendly_id,
        o.linehaul_channel_id,
        o.delivery_time_days
    FROM {{ ref('fact_order') }} AS o
    LEFT JOIN {{ ref('dim_user') }} AS r ON o.user_id = r.user_id
    WHERE TRUE
        AND o.created_ts_msk >= '2022-05-19'
        AND o.next_effective_ts_msk IS NULL
        AND NOT COALESCE(r.fake, FALSE)
        AND r.next_effective_ts_msk IS NULL
),

admin AS (
    SELECT
        admin_id,
        email
    FROM {{ ref('dim_user_admin') }}
),


certified AS (
    SELECT
        CASE WHEN certification_final_price > 0 THEN 1 ELSE 0 END AS certified,
        owner_moderator_id,
        order_id
    FROM
        (
            SELECT
                owner_moderator_id,
                order_id,
                certification_final_price,
                MAX(event_ts_msk) OVER (PARTITION BY order_id) AS max_ts,
                event_ts_msk
            FROM {{ ref('fact_order_change') }}
        )
    WHERE max_ts = event_ts_msk
),


stg1 AS (
    SELECT
        o.order_id,
        o.status,
        o.sub_status,
        o.event_ts_msk,
        c.certified,
        FIRST_VALUE(ao.email) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.status_id DESC, o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.status_id DESC, o.sub_status_id DESC, o.event_ts_msk DESC) AS current_sub_status,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status) AS min_sub_status_ts,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status) AS min_status_ts,
        LEAD(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.status_id, o.sub_status_id, o.event_ts_msk) AS lead_sub_status_ts,
        LEAD(o.status) OVER (PARTITION BY o.order_id ORDER BY o.status_id, o.sub_status_id, o.event_ts_msk) AS lead_status,
        LEAD(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.status_id, o.sub_status_id, o.event_ts_msk) AS lead_sub_status,
        LAG(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.status_id, o.sub_status_id, o.event_ts_msk) AS lag_sub_status_ts,
        LAG(o.status) OVER (PARTITION BY o.order_id ORDER BY o.status_id, o.sub_status_id,o.event_ts_msk) AS lag_status,
        LAG(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.status_id, o.sub_status_id, o.event_ts_msk) AS lag_sub_status
    FROM {{ ref('fact_order_statuses') }} AS o
    LEFT JOIN certified AS c ON o.order_id = c.order_id
    LEFT JOIN admin AS ao ON c.owner_moderator_id = ao.admin_id
),

stg2 AS (
    SELECT
        order_id,
        status,
        sub_status,
        event_ts_msk,
        owner_moderator_email,
        current_status,
        current_sub_status,
        certified,
        IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL, TRUE, FALSE) AS flg,
        date(COALESCE(lead_sub_status_ts, CURRENT_TIMESTAMP())) AS lead_sub_status_ts,
        date(COALESCE(
            FIRST_VALUE(CASE WHEN lead_status != status THEN lead_sub_status_ts END)
            OVER (PARTITION BY order_id, status ORDER BY lead_status != status, lead_sub_status_ts),
            CURRENT_TIMESTAMP())) AS lead_status_ts,
        date(FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status, sub_status
            ORDER BY event_ts_msk)) AS first_substatus_event_msk,
        date(FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status
            ORDER BY event_ts_msk)) AS first_status_event_msk
    FROM stg1
    WHERE (lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL)
        OR (lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL)
),

linehaul AS (
    SELECT DISTINCT
        id,
        name,
        min_days,
        max_days,
        channel_type
    FROM {{ ref('linehaul_channels') }}
),

merchant_order AS (
    SELECT
        order_id,
        MAX(manufacturing_days) AS manufacturing_days
    FROM {{ ref('fact_merchant_order') }}
    GROUP BY 1
),

orders_hist AS (
    SELECT
        order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        MAX(certified) AS certified,
        MAX(IF(status = 'manufacturing', datediff(lead_status_ts, first_status_event_msk), NULL)) AS days_in_manufacturing,
        MAX(IF(status = 'shipping', datediff(DATE(lead_status_ts), DATE(first_status_event_msk)), NULL)) AS days_in_shipping,
        MAX(IF(sub_status = 'pickupRequestSentToLogisticians', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_pickup_request_sent_to_logisticians,
        MAX(IF(sub_status = 'pickedUpByLogisticians', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_picked_up_by_logisticians,
        MAX(IF(sub_status = 'arrivedAtLogisticsWarehouse', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_arrived_at_logistics_warehouse,
        MAX(IF(sub_status = 'departedFromLogisticsWarehouse', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_departed_from_logistics_warehouse,
        MAX(IF(sub_status = 'arrivedAtStateBorder', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_arrived_at_state_border,
        MAX(IF(sub_status = 'departedFromStateBorder', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_departed_from_state_border,
        MAX(IF(sub_status IN ('arrivedAtCustoms', 'arrivedAtDestinations'), datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_arrived_at_customs,
        MAX(IF(sub_status = 'documentReceived', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_document_received,
        MAX(IF(sub_status = 'customsDeclarationFiled', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_customs_declaration_filed,
        MAX(IF(sub_status = 'customsDeclarationReleased', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_customs_declaration_released,
        MAX(IF(sub_status = 'uploadToTemporaryWarehouse', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_upload_to_temporary_warehouse,
        MAX(IF(sub_status = 'delivering', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_delivering,
        MAX(IF(sub_status = 'delivered', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_delivered,
        MAX(IF(sub_status = 'requestedClosingDocuments', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_requested_closing_documents,
        MAX(IF(sub_status = 'receivedClosingDocuments', datediff(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_received_closing_documents,
        MAX(IF(status = 'manufacturing', first_status_event_msk, NULL)) AS manufacturing,
        MAX(IF(status = 'shipping', first_status_event_msk, NULL)) AS shipping,
        MAX(IF(status = 'shipping' AND sub_status = 'pickupRequestSentToLogisticians',
            first_substatus_event_msk, NULL)) AS pickup_request_sent_to_logisticians,
        MAX(IF(status = 'shipping' AND sub_status = 'pickedUpByLogisticians', first_substatus_event_msk, NULL)) AS picked_up_by_logisticians,
        MAX(IF(status = 'shipping' AND sub_status = 'arrivedAtLogisticsWarehouse', first_substatus_event_msk, NULL))
        AS arrived_at_logistics_warehouse,
        MAX(IF(status = 'shipping' AND sub_status = 'departedFromLogisticsWarehouse',
            first_substatus_event_msk, NULL)) AS departed_from_logistics_warehouse,
        MAX(IF(status = 'shipping' AND sub_status = 'arrivedAtStateBorder', first_substatus_event_msk, NULL)) AS arrived_at_state_border,
        MAX(IF(status = 'shipping' AND sub_status = 'departedFromStateBorder', first_substatus_event_msk, NULL)) AS departed_from_state_border,
        MAX(IF(status = 'shipping' AND sub_status IN (
            'arrivedAtCustoms', 'arrivedAtDestinations'), first_substatus_event_msk, NULL)) AS arrived_at_customs,
        MAX(IF(status = 'shipping' AND sub_status = 'documentReceived', first_substatus_event_msk, NULL)) AS document_received,
        MAX(IF(status = 'shipping' AND sub_status = 'customsDeclarationFiled', first_substatus_event_msk, NULL)) AS customs_declaration_filed,
        MAX(IF(status = 'shipping' AND sub_status = 'customsDeclarationReleased', first_substatus_event_msk, NULL)) AS customs_declaration_released,
        MAX(IF(status = 'shipping' AND sub_status = 'uploadToTemporaryWarehouse', first_substatus_event_msk, NULL)) AS upload_to_temporary_warehouse,
        MAX(IF(status = 'shipping' AND sub_status = 'delivering', first_substatus_event_msk, NULL)) AS delivering,
        MAX(IF(status = 'shipping' AND sub_status = 'delivered', first_substatus_event_msk, NULL)) AS delivered,
        MAX(IF(status = 'shipping' AND sub_status = 'requestedClosingDocuments', first_substatus_event_msk, NULL)) AS requested_closing_documents,
        MAX(IF(status = 'shipping' AND sub_status = 'receivedClosingDocuments', first_substatus_event_msk, NULL)) AS received_closing_documents
    FROM stg2
    WHERE flg = TRUE AND status IN ('manufacturing', 'shipping')
    GROUP BY 1, 2, 3, 4
)

SELECT DISTINCT
    o.order_id,
    o.friendly_id,
    DATE(o.created_ts_msk) AS created_day,
    o.created_ts_msk,
    o.linehaul_channel_id,
    o.delivery_time_days,
    l.name AS linehaul_channel_name,
    l.min_days AS linehaul_min_days,
    l.max_days AS linehaul_max_days,
    l.channel_type AS linehaul_channel_type,
    mo.manufacturing_days,
    oh.certified,
    oh.current_status,
    oh.current_sub_status,
    oh.owner_moderator_email,
    oh.days_in_manufacturing,
    oh.days_in_shipping,
    oh.days_in_pickup_request_sent_to_logisticians,
    oh.days_in_picked_up_by_logisticians,
    oh.days_in_arrived_at_logistics_warehouse,
    oh.days_in_departed_from_logistics_warehouse,
    oh.days_in_arrived_at_state_border,
    oh.days_in_departed_from_state_border,
    oh.days_in_arrived_at_customs,
    oh.days_in_document_received,
    oh.days_in_customs_declaration_filed,
    oh.days_in_customs_declaration_released,
    oh.days_in_upload_to_temporary_warehouse,
    oh.days_in_delivering,
    oh.days_in_delivered,
    oh.days_in_requested_closing_documents,
    oh.days_in_received_closing_documents,
    oh.manufacturing,
    oh.shipping,
    oh.pickup_request_sent_to_logisticians,
    oh.picked_up_by_logisticians,
    oh.arrived_at_logistics_warehouse,
    oh.departed_from_logistics_warehouse,
    oh.arrived_at_state_border,
    oh.departed_from_state_border,
    oh.arrived_at_customs,
    oh.document_received,
    oh.customs_declaration_filed,
    oh.customs_declaration_released,
    oh.upload_to_temporary_warehouse,
    oh.delivering,
    oh.delivered,
    oh.requested_closing_documents,
    oh.received_closing_documents
FROM orders AS o
INNER JOIN orders_hist AS oh ON o.order_id = oh.order_id
LEFT JOIN linehaul AS l ON o.linehaul_channel_id = l.id
LEFT JOIN merchant_order AS mo ON o.order_id = mo.order_id
WHERE oh.manufacturing IS NOT NULL
