{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}

WITH requests AS (
    SELECT
        request_id,
        MAX(is_joompro_employee) OVER (PARTITION BY user_id) AS is_joompro_employee
    FROM {{ ref('fact_user_request') }}
),

orders AS (
    SELECT
        o.order_id,
        o.created_ts_msk,
        o.friendly_id
    FROM {{ ref('fact_order') }} AS o
    LEFT JOIN requests AS r ON o.request_id = r.request_id
    WHERE TRUE
        AND o.created_ts_msk >= '2022-05-19'
        AND o.next_effective_ts_msk IS NULL
        AND NOT COALESCE(r.is_joompro_employee, FALSE)
),

admin AS (
    SELECT
        admin_id,
        email,
        role,
        created_ts_msk
    FROM {{ ref('dim_user_admin') }}
),


order_owner AS (
    select distinct 
    order_id, owner_moderator_email, owner_role
    from
    (SELECT
        o.order_id,
        FIRST_VALUE(ao.email) OVER (partition by o.order_id order by event_ts_msk desc) AS owner_moderator_email,
        FIRST_VALUE(s.role) OVER (partition by o.order_id order by event_ts_msk desc) AS owner_role
    FROM {{ ref('fact_order_change') }} AS o
    LEFT JOIN admin AS ao ON o.owner_moderator_id = ao.admin_id
    LEFT JOIN {{ ref('support_roles') }} AS s ON ao.email = s.email
    )
),

rfq_requests AS (
    SELECT
        rfq_request_id,
        created_ts_msk AS request_created_ts_msk,
        sent_ts_msk AS request_sent_ts_msk,
        status AS sent_status,
        category_id,
        category_name,
        order_id
    FROM {{ ref('fact_rfq_requests') }}
    WHERE next_effective_ts_msk IS NULL
),

response AS (
    SELECT
        order_rfq_response_id,
        rfq_request_id,
        created_ts_msk AS response_created_ts_msk,
        status AS response_status,
        reject_reason,
        merchant_id,
        product_id
    FROM {{ ref('fact_rfq_response') }}
    WHERE next_effective_ts_msk IS NULL
),

rfq_1 as (SELECT
    rq.order_id,
    rq.rfq_request_id,
    rq.request_created_ts_msk,
    rq.request_sent_ts_msk,
    rp.response_created_ts_msk,
    rq.sent_status,
    rp.order_rfq_response_id,
    rp.response_status,
    rp.reject_reason,
    rp.merchant_id,
    product_id
FROM rfq_requests AS rq
LEFT JOIN response AS rp ON rq.rfq_request_id = rp.rfq_request_id
),

rfq as (
    SELECT 
        order_id,
        'rfq' as status,
        'rfq_created' as sub_status,
        min(request_created_ts_msk) over (partition by order_id) as event_ts_msk,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        0 as converted,
        0 as rfq_converted
    from rfq_1
    union all 
        SELECT 
        order_id,
        'rfq' as status,
        'rfq_sent' as sub_status,
        min(request_sent_ts_msk) over (partition by order_id) as event_ts_msk,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        0 as converted,
        0 as rfq_converted
    from rfq_1
    union all 
        SELECT 
        order_id,
        'rfq' as status,
        'rfq_response' as sub_status,
        min(response_created_ts_msk) over (partition by order_id) as event_ts_msk,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        0 as converted,
        0 as rfq_converted
    from rfq_1
),

order_products as (
    select distinct
        order_id, id as product_id
        FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }} as o
    JOIN (select _id, orderId as order_id from mongo.b2b_core_merchant_orders_v2_daily_snapshot) m ON m._id = o.merchOrdId
),

orders_statuses as (
    select 
        o.order_id,
        o.status,
        COALESCE(o.sub_status, o.status) as sub_status,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS event_ts_msk,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        max(case when rfq_1.product_id = op.product_id then 1 else 0 end) OVER (PARTITION BY o.order_id) AS converted,
        max(case when rfq_1.product_id = op.product_id then 1 else 0 end) OVER (PARTITION BY o.order_id, rfq_request_id) AS rfq_converted
      FROM b2b_mart.fact_order_change o
      left join rfq_1 on o.order_id = rfq_1.order_id
      left join order_products op on o.order_id = op.order_id
),

products as (
    select 
    op.order_id, 
    count(distinct product_id) as total_products,
    count(distinct case when rfq_1.product_id = op.product_id then product_id end) as rfq_products
    from order_products op
    left join rfq_1 on op.order_id = rfq_1.order_id
    group by op.order_id
),

stg1 AS (
    SELECT
        o.order_id,
        o.status,
        COALESCE(o.sub_status, o.status) as sub_status,
        o.event_ts_msk,
        owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        converted,
        owner_role,
        rfq_converted,
        total_products,
        rfq_products
    FROM (select * from orders_statuses union all select * from rfq) AS o
    LEFT JOIN order_owner AS ao ON o.order_id = ao.order_id
    LEFT JOIN  products as p on p.order_id = o.order_id
),


orders_hist AS (
    SELECT
        order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        owner_role,
        max(converted) as converted,
        max(rfq_converted) as rfq_converted,
        max(total_products) as total_products,
        max(rfq_products) as rfq_products,
        MAX(IF(status = 'selling' AND sub_status = 'new', event_ts_msk, '')) AS new_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'priceEstimation', event_ts_msk, '')) AS price_estimation_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'negotiation', event_ts_msk, '')) AS negotiation_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'finalPricing', event_ts_msk, '')) AS final_pricing_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'signingAndPayment', event_ts_msk, '')) AS signing_and_payment_ts_msk,
        MAX(IF(status = 'rfq' AND sub_status = 'rfq_created', event_ts_msk, '')) AS rfq_created_ts_msk,
        MAX(IF(status = 'rfq' AND sub_status = 'rfq_sent', event_ts_msk, '')) AS rfq_sent_ts_msk,
        MAX(IF(status = 'rfq' AND sub_status = 'rfq_response', event_ts_msk, '')) AS rfq_response_ts_msk,
        MAX(IF(status = 'manufacturing', event_ts_msk, '')) AS manufacturing_ts_msk,
        MAX(IF(status = 'cancelled', event_ts_msk, '')) AS cancelled_ts_msk
    FROM stg1
    WHERE status in ('selling', 'rfq', 'manufacturing', 'cancelled')
    GROUP BY order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        rfq_request_id,
        order_rfq_response_id,
        response_status,
        reject_reason,
        owner_role
)


SELECT order_id,
    owner_moderator_email,
    current_status,
    current_sub_status,
    rfq_request_id,
    order_rfq_response_id,
    response_status,
    reject_reason,
    new_ts_msk,
    price_estimation_ts_msk,
    negotiation_ts_msk,
    final_pricing_ts_msk,
    signing_and_payment_ts_msk,
    rfq_created_ts_msk,
    rfq_sent_ts_msk,
    rfq_response_ts_msk,
    manufacturing_ts_msk,
    cancelled_ts_msk,
    owner_role,
    converted,
    rfq_converted,
    total_products,
    rfq_products,
    (unix_timestamp(substring(signing_and_payment_ts_msk, 0, 19) ,"yyyy-MM-dd HH:mm:ss")-unix_timestamp(substring(signing_and_payment_ts_msk, 0, 19),"yyyy-MM-dd HH:mm:ss"))/(3600) as time_final_pricing,
    (unix_timestamp(substring(rfq_response_ts_msk, 0, 19) ,"yyyy-MM-dd HH:mm:ss")-unix_timestamp(substring(rfq_sent_ts_msk, 0, 19) ,"yyyy-MM-dd HH:mm:ss"))/(3600) as time_rfq_response,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER by order_rfq_response_id) as order_rn,
    ROW_NUMBER() OVER (PARTITION BY order_id, rfq_request_id ORDER BY order_rfq_response_id) as rfq_rn,
    CASE WHEN cancelled_ts_msk = '' THEN ''
        WHEN manufacturing_ts_msk != '' THEN 'manufacturing'
        WHEN signing_and_payment_ts_msk != '' THEN 'signing_and_payment'
        WHEN final_pricing_ts_msk != '' THEN 'final_pricing'
        WHEN rfq_response_ts_msk != '' AND negotiation_ts_msk < rfq_response_ts_msk THEN 'rfq_response'
        WHEN negotiation_ts_msk != '' AND negotiation_ts_msk < rfq_response_ts_msk THEN 'negotiation'
        WHEN rfq_response_ts_msk != '' AND price_estimation_ts_msk < rfq_response_ts_msk AND negotiation_ts_msk != '' THEN 'rfq_response'
        WHEN price_estimation_ts_msk != '' AND price_estimation_ts_msk < rfq_response_ts_msk AND negotiation_ts_msk != '' THEN 'price_estimation'
        WHEN price_estimation_ts_msk != '' AND new_ts_msk < rfq_response_ts_msk AND negotiation_ts_msk != '' AND rfq_response_ts_msk != '' THEN 'rfq_response'
        WHEN negotiation_ts_msk != '' THEN 'negotiation'
        WHEN price_estimation_ts_msk != '' THEN 'price_estimation'
        WHEN new_ts_msk != '' THEN 'new'
        END as cancelled_after
FROM
(SELECT DISTINCT
    order_id,
    owner_moderator_email,
    current_status,
    current_sub_status,
    rfq_request_id,
    order_rfq_response_id,
    response_status,
    reject_reason,
    new_ts_msk,
    price_estimation_ts_msk,
    negotiation_ts_msk,
    final_pricing_ts_msk,
    signing_and_payment_ts_msk,
    rfq_created_ts_msk,
    rfq_sent_ts_msk,
    rfq_response_ts_msk,
    manufacturing_ts_msk,
    cancelled_ts_msk,
    owner_role,
    converted,
    rfq_converted,
    total_products,
    rfq_products
from orders_hist
WHERE COALESCE(new_ts_msk, price_estimation_ts_msk, negotiation_ts_msk, final_pricing_ts_msk, signing_and_payment_ts_msk) IS NOT NULL
AND COALESCE(new_ts_msk, price_estimation_ts_msk, negotiation_ts_msk, final_pricing_ts_msk, signing_and_payment_ts_msk) != ''
)
