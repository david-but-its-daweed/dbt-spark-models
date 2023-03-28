{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
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
        o.friendly_id,
        o.user_id,
        o.min_manufactured_ts_msk
    FROM {{ ref('fact_order') }} AS o
    LEFT JOIN requests AS r ON o.request_id = r.request_id
    WHERE TRUE
        AND o.created_ts_msk >= '2022-05-19'
        AND o.next_effective_ts_msk IS NULL
        AND NOT COALESCE(r.is_joompro_employee, FALSE)
),

internal_products as (
    select product_id, order_id, product_type,
        row_number() over (partition by user_id, product_id order by min_manufactured_ts_msk is null, min_manufactured_ts_msk) 
                as user_product_number
        from
    (
    select distinct * 
    from 
    (SELECT DISTINCT
        product_id, 
        mo.order_id, 
        o.user_id, 
        max(type) over (partition by product_id) as product_type,
        o.min_manufactured_ts_msk
    FROM (
        select orderId as order_id, _id as merchant_order_id
        from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
        ) mo
    LEFT JOIN (
        select _id as product_id, type, merchOrdId as merchant_order_id
        from {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
        ) op on mo.merchant_order_id = op.merchant_order_id
    LEFT JOIN orders o on o.order_id = mo.order_id
    union all 
    SELECT DISTINCT
        product_id, mo.order_id, o.user_id, 
        max(product_type) over (partition by product_id) as product_type,
        o.min_manufactured_ts_msk
    FROM {{ ref('fact_merchant_order') }} mo
    LEFT JOIN orders o on o.order_id = mo.order_id
    WHERE next_effective_ts_msk IS NULL and product_id is not null
    )
)
    
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
        top_rfq,
        category_id,
        category_name,
        order_id
    FROM {{ ref('fact_rfq_requests') }}
    WHERE next_effective_ts_msk IS NULL
),

response AS (
    SELECT
        order_rfq_response_id,
        documents_attached,
        rfq_request_id,
        created_ts_msk AS response_created_ts_msk,
        status AS response_status,
        reject_reason,
        merchant_id,
        product_id
    FROM {{ ref('fact_rfq_response') }}
    WHERE next_effective_ts_msk IS NULL
),

rfq as (SELECT
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
    top_rfq,
    category_id,
    category_name,
    case when documents_attached > 0 then True else False end as documents_attached,
    product_id
FROM rfq_requests AS rq
LEFT JOIN response AS rp ON rq.rfq_request_id = rp.rfq_request_id
),

price as (
    select min(least(prc[0], prc[1], prc[2])) as price, _id as product_id
    from
    (select _id, prcQtyDis from {{ source('mongo', 'b2b_core_product_appendixes_daily_snapshot') }}
    where prcQtyDis[0] is not null) a
    left join 
    (select pId, prc from {{ source('mongo', 'b2b_core_variant_appendixes_daily_snapshot') }}
    where prc[0] is not null) b on _id = pId
    group by _id
),
    
expensive as (
    select distinct 
    order_rfq_response_id as id, 
    price > price_standart as expensive,
    price_min != price_max as different_prices
    from
    (
    select avg(price) over (partition by order_id, rfq_request_id) as price_standart,
    min(price) over (partition by order_id, rfq_request_id) as price_min,
    max(price) over (partition by order_id, rfq_request_id) as price_max,
    price, order_id, rfq_request_id, order_rfq_response_id
    from
    (select distinct order_id, rfq_request_id, order_rfq_response_id, price
    from rfq r
    left join price p on r.product_id = p.product_id
    )
    )
),

order_products as (
    select order_id, product_id, max(order_product) as order_product, max(rfq_product) as rfq_product
    from
    (select distinct
        order_id, id as product_id, 1 as order_product, 0 as rfq_product
    FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }} as o
    JOIN (select _id, orderId as order_id from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}) m ON m._id = o.merchOrdId
    UNION ALL 
    select distinct order_id, product_id, 0 as order_product, 1 as rfq_product 
    from rfq)
    group by order_id, product_id
),

products as (
    select order_id, 
    sum(order_product) as order_products,
    sum(order_product*rfq_product) as rfq_converted_products,
    sum(rfq_product) as rfq_products
    from order_products
    group by order_id
),

stg1 AS (
    SELECT
        o.order_id,
        o.status,
        COALESCE(o.sub_status, o.status) as sub_status,
        o.event_ts_msk,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, COALESCE(o.sub_status, o.status)) AS event_ts,
        owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        owner_role
    FROM {{ ref('fact_order_change') }} AS o
    LEFT JOIN order_owner AS ao ON o.order_id = ao.order_id
),



orders_hist AS (
    SELECT
        order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        owner_role,
        MAX(IF(status = 'selling' AND sub_status = 'new', event_ts, '')) AS new_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'priceEstimation', event_ts, '')) AS price_estimation_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'negotiation', event_ts, '')) AS negotiation_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'finalPricing', event_ts, '')) AS final_pricing_ts_msk,
        MAX(IF(status = 'selling' AND sub_status = 'signingAndPayment', event_ts, '')) AS signing_and_payment_ts_msk,
        MAX(IF(status = 'manufacturing', event_ts, '')) AS manufacturing_ts_msk,
        MAX(IF(status = 'cancelled', event_ts, '')) AS cancelled_ts_msk
    FROM stg1
    WHERE status in ('selling', 'rfq', 'manufacturing', 'cancelled')
    GROUP BY order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        owner_role
)


select
    rfq.order_id,
    owner_moderator_email,
    current_status,
    current_sub_status,
    rfq_request_id,
    order_rfq_response_id,
    rfq.product_id,
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
    rfq_converted_products,
    order_products,
    rfq_products,
    documents_attached,
    merchant_id,
    top_rfq,
    category_id,
    category_name,
    (unix_timestamp(substring(signing_and_payment_ts_msk, 0, 19) ,"yyyy-MM-dd HH:mm:ss")-unix_timestamp(substring(signing_and_payment_ts_msk, 0, 19),"yyyy-MM-dd HH:mm:ss"))/(3600) as time_final_pricing,
    (unix_timestamp(substring(rfq_response_ts_msk, 0, 19) ,"yyyy-MM-dd HH:mm:ss")-unix_timestamp(substring(rfq_sent_ts_msk, 0, 19) ,"yyyy-MM-dd HH:mm:ss"))/(3600) as time_rfq_response,
    RANK() OVER (PARTITION BY rfq.order_id ORDER by (rfq_response_ts_msk = "" or order_rfq_response_id is null), rfq_response_ts_msk, order_rfq_response_id) as order_rn,
    RANK() OVER (PARTITION BY rfq.order_id, rfq_request_id ORDER BY (rfq_response_ts_msk = "" or order_rfq_response_id is null), rfq_response_ts_msk, order_rfq_response_id) as rfq_rn,
    RANK() OVER (PARTITION BY rfq.order_id, rfq_request_id, merchant_id  ORDER BY (rfq_response_ts_msk = "" or order_rfq_response_id is null), rfq_response_ts_msk, order_rfq_response_id) as rfq_merchant_rn,
    MAX(order_rfq_response_id) OVER (PARTITION BY rfq.order_id, rfq_request_id) as max_response,
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
        END as cancelled_after,
    amount,
    product_type,
    user_product_number
FROM
(
select distinct
oh.order_id,
oh.owner_moderator_email,
oh.owner_role,
oh.current_status, 
oh.current_sub_status,
oh.new_ts_msk,
oh.price_estimation_ts_msk,
oh.negotiation_ts_msk,
oh.final_pricing_ts_msk,
oh.signing_and_payment_ts_msk,
oh.manufacturing_ts_msk,
oh.cancelled_ts_msk,
op.product_id,
op.order_product,
op.rfq_product,
op.order_product*op.rfq_product as converted,
rfq.rfq_request_id,
rfq.request_created_ts_msk as rfq_created_ts_msk,
rfq.request_sent_ts_msk as rfq_sent_ts_msk,
rfq.response_created_ts_msk as rfq_response_ts_msk,
rfq.sent_status,
rfq.order_rfq_response_id,
rfq.response_status,
case when rfq.reject_reason = 'expensive' and expensive and different_prices then 'expensive (other options cheaper)' else rfq.reject_reason end as reject_reason,
rfq.merchant_id,
rfq.category_id,
rfq.category_name,
rfq.documents_attached,
p.order_products,
p.rfq_converted_products,
p.rfq_products,
top_rfq,
opp.amount
from orders_hist oh
left join order_products op on oh.order_id = op.order_id
left join rfq on oh.order_id = rfq.order_id
left join products p on oh.order_id = p.order_id
left join expensive e on rfq.order_rfq_response_id = e.id
left join {{ ref('order_product_prices') }} opp on opp.order_id = oh.order_id and op.product_id = opp.product_id
where (op.product_id = rfq.product_id or op.product_id is null or rfq.product_id is null)
) rfq
left join internal_products ip on ip.product_id = rfq.product_id and rfq.order_id = ip.order_id
