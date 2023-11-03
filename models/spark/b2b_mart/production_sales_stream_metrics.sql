{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


WITH requests AS (
    SELECT
        request_id,
        MAX(is_joompro_employee) OVER (PARTITION BY user_id) AS is_joompro_employee
    FROM {{ ref('fact_user_request') }}
),


customers as (
    select distinct
    user_id,
    grade,
    grade_probability
    from {{ ref('fact_customers') }}
),

users_hist as (
  select
    user_id,
    min(partition_date_msk) as validated_date
    from {{ ref('fact_user_change') }}
    where validation_status = 'validated'
    group by user_id
),


validation_status as (
    SELECT
    10 AS id,
    'initial' AS status
    UNION ALL
    SELECT
        20 AS id,
        'needsValidation' AS status
    UNION ALL
    SELECT
        25 AS id,
        'onHold' AS status
    UNION ALL
    SELECT
        30 AS id,
        'validated' AS status
    UNION ALL
    SELECT
        40 AS id,
    'rejected' AS status
),

reject_reasons as (
    SELECT
    1010 AS id,
    'noFeedback' AS reason
UNION ALL
SELECT
    1020 AS id,
    'unsuitableDeliveryTerms' AS reason
UNION ALL
SELECT
    1030 AS id,
    'unsuitableFinancialTerms' AS reason
UNION ALL
SELECT
    1040 AS id,
    'unsuitableWarehouseTerms' AS reason
UNION ALL
SELECT
    1050 AS id,
    'unsuitableVolume' AS reason
UNION ALL
SELECT
    1060 AS id,
    'needsTimeForDecision' AS reason
UNION ALL
SELECT
    1070 AS id,
    'didNotSendRequest' AS reason
UNION ALL
SELECT
    1080 AS id,
    'deadRequest' AS reason
UNION ALL
SELECT
    1090 AS id,
    'uncommunicativeClient' AS reason
UNION ALL
SELECT
    1100 AS id,
    'noLegalEntity' AS reason
UNION ALL
SELECT
    1110 AS id,
    'other' AS reason
),

users as (
select 
    du.user_id, 
    vs.status as validation_status, 
    rr.reason as reject_reason,
    date(created_ts_msk) as created_date,
    validated_date
    from {{ ref('dim_user') }} du
    left join validation_status vs on du.validation_status = vs.id
    left join reject_reasons rr on du.reject_reason = rr.id
    left join users_hist uh on uh.user_id = du.user_id
    where next_effective_ts_msk is null
),

admin AS (
    SELECT
        admin_id,
        a.email,
        s.role as owner_role
    FROM {{ ref('dim_user_admin') }} a
    LEFT JOIN {{ ref('support_roles') }} s on a.email = s.email
),

not_jp_users AS (
  SELECT DISTINCT f.user_id, f.order_id
  FROM {{ ref('fact_order') }} f
  LEFT JOIN {{ ref('fact_user_request') }} u ON f.user_id = u.user_id
  WHERE is_joompro_employee = FALSE or is_joompro_employee is null
    AND next_effective_ts_msk is null
),

stg1 AS (
    SELECT
        o.order_id,
        nu.user_id,
        o.status,
        o.sub_status,
        o.event_ts_msk,
        first_value(ao.email) OVER (PARTITION BY o.order_id, o.status, o.sub_status ORDER BY o.event_ts_msk desc) as owner_email,
        first_value(ao.owner_role) OVER (PARTITION BY o.order_id, o.status, o.sub_status ORDER BY o.event_ts_msk desc) as owner_role,
        FIRST_VALUE(o.total_final_price) OVER (PARTITION BY o.order_id, o.status, o.sub_status ORDER BY o.event_ts_msk) AS final_gmv,
        FIRST_VALUE(ao.email) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS min_status_ts,
        LEAD(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status_ts,
        LEAD(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status,
        LEAD(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status,
        LAG(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_status_ts,
        LAG(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_status,
        LAG(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_sub_status
    FROM {{ ref('fact_order_change') }} AS o
    INNER JOIN not_jp_users nu ON o.order_id = nu.order_id
    LEFT JOIN admin AS ao ON o.owner_moderator_id = ao.admin_id
),

stg2 AS (
    SELECT
        order_id,
        user_id,
        owner_email,
        owner_role,
        status,
        sub_status,
        event_ts_msk,
        owner_moderator_email,
        current_status,
        current_sub_status,
        final_gmv,
        lead_status,
        lead_sub_status,
        COALESCE(lead_status_ts, CURRENT_TIMESTAMP()) AS lead_status_ts,
        IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL,
            COALESCE(lead_status_ts, TIMESTAMP(CURRENT_TIMESTAMP())), NULL) IS NOT NULL AS flg,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status, sub_status
            ORDER BY CASE WHEN IF(lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL,
                event_ts_msk, NULL) IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk) AS first_substatus_event_msk
    FROM stg1
    WHERE IF(lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL, event_ts_msk, NULL) IS NOT NULL
        OR IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL,
            COALESCE(lead_status_ts, CURRENT_TIMESTAMP()), NULL) IS NOT NULL
),

orders_hist AS (
    SELECT
        order_id,
        user_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        owner_email,
        owner_role,
        MAX(CASE WHEN current_status = status AND current_sub_status = sub_status THEN final_gmv ELSE 0 END) AS gmv,
        MAX(IF(status = 'selling', first_substatus_event_msk, null)) AS selling_day,
        CASE WHEN MAX(IF(status = 'manufacturing', first_substatus_event_msk, null)) is not null then 1 else 0 end as was_in_manufacturing,
        CASE WHEN 
            MAX(IF(status = 'cancelled', first_substatus_event_msk, null)) > MAX(IF(status = 'manufacturing', first_substatus_event_msk, null))
            THEN 1 
            WHEN MAX(IF(status = 'cancelled', first_substatus_event_msk, null)) <= MAX(IF(status = 'manufacturing', first_substatus_event_msk, null))
            THEN 0 end as cancelled_in_selling,
        MAX(IF(status = 'cancelled', sub_status, null)) AS cancelled_reason,
        MAX(IF(sub_status = 'new', DATEDIFF(lead_status_ts,first_substatus_event_msk), null)) AS days_in_selling_new,
        MAX(CASE WHEN sub_status = 'new' AND lead_sub_status = 'priceEstimation' THEN 1 
            WHEN sub_status = 'new' AND lead_status = 'cancelled' THEN 2 ELSE 0 END) AS new_valid,
        MAX(IF(sub_status = 'priceEstimation', DATEDIFF(lead_status_ts,first_substatus_event_msk), null)) AS days_in_selling_price_estimation,
        MAX(CASE WHEN sub_status = 'priceEstimation' AND lead_sub_status = 'negotiation' THEN 1 
            WHEN sub_status = 'priceEstimation' AND lead_status = 'cancelled' THEN 2 ELSE 0 END) AS price_estimation_valid,
        MAX(IF(sub_status = 'negotiation', DATEDIFF(lead_status_ts,first_substatus_event_msk), null)) AS days_in_selling_negotiation,
        MAX(CASE WHEN sub_status = 'negotiation' AND lead_sub_status = 'finalPricing' THEN 1 
            WHEN sub_status = 'negotiation' AND lead_status = 'cancelled' THEN 2 ELSE 0 END) AS negotiation_valid,
        MAX(IF(sub_status = 'finalPricing', DATEDIFF(lead_status_ts,first_substatus_event_msk), null)) AS days_in_selling_final_pricing,
        MAX(CASE WHEN sub_status = 'finalPricing' AND lead_sub_status = 'signingAndPayment' THEN 1 
            WHEN sub_status = 'finalPricing' AND lead_status = 'cancelled' THEN 2 ELSE 0 END) AS final_pricing_valid,
        MAX(IF(sub_status = 'signingAndPayment', DATEDIFF(lead_status_ts,first_substatus_event_msk), null)) AS days_in_selling_signing_and_payment,
        MAX(CASE WHEN sub_status = 'signingAndPayment' AND lead_status = 'manufacturing' THEN 1 
            WHEN sub_status = 'signingAndPayment' AND lead_status = 'cancelled' THEN 2 ELSE 0 END) AS signing_and_payment_valid
    FROM stg2
    WHERE flg = TRUE
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

rfq as (select
    order_id,
    case when max(rfq_created_ts_msk is not null) then 'with rfq' else 'without rfq' end as rfq,
    case when max(rfq_created_ts_msk is not null and rfq_converted_products > 0) then 'rfq converted' else 'own sourcing' end as sourcing,
    sum(case when order_rn = 1 then rfq_converted_products else 0 end) as rfq_converted_products,
    sum(case when order_rn = 1 then rfq_products else 0 end) as rfq_products
     from {{ ref('rfq_metrics') }}
        where product_id is not null and product_id != ""
     group by 1),
     
order_products as (
    select order_id, count(distinct product_id) as order_products
    from
    (select distinct
        order_id, id as product_id
    FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }} as o
    JOIN (select _id, orderId as order_id from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}) m ON m._id = o.merchOrdId
    )
    group by order_id
)

SELECT
    DATE(oh.selling_day) AS partition_date,
    oh.order_id,
    u.user_id, 
    validation_status, 
    reject_reason,
    created_date,
    validated_date,
    owner_email,
    owner_role,
    coalesce(rfq, "without rfq") as rfq,
    coalesce(sourcing, 'own sourcing') as sourcing,
    rfq_products,
    rfq_converted_products,
    order_products,
    oh.gmv,
    oh.was_in_manufacturing,
    oh.cancelled_in_selling,
    oh.cancelled_reason,
    oh.current_status,
    oh.current_sub_status,
    oh.owner_moderator_email,
    oh.days_in_selling_new,
    oh.days_in_selling_price_estimation,
    oh.days_in_selling_negotiation,
    oh.days_in_selling_final_pricing,
    oh.days_in_selling_signing_and_payment,
    CASE WHEN oh.new_valid = 1 THEN 'valid'
        WHEN oh.new_valid = 2 THEN 'cancelled'
        ELSE 'not valid' END AS new,
    CASE WHEN oh.price_estimation_valid = 1 THEN 'valid'
        WHEN oh.price_estimation_valid = 2 THEN 'cancelled'
        ELSE 'not valid' END AS price_estimation,
    CASE WHEN oh.negotiation_valid = 1 THEN 'valid'
        WHEN oh.negotiation_valid = 2 THEN 'cancelled'
        ELSE 'not valid' END AS negotiation,
    CASE WHEN oh.final_pricing_valid = 1 THEN 'valid'
        WHEN oh.final_pricing_valid = 2 THEN 'cancelled'
        ELSE 'not valid' END AS final_pricing,
    CASE WHEN oh.signing_and_payment_valid = 1 THEN 'valid'
        WHEN oh.signing_and_payment_valid = 2 THEN 'cancelled'
        ELSE 'not valid' END AS signing_and_payment,
   grade,
   grade_probability
FROM orders_hist AS oh
inner join users u on oh.user_id = u.user_id
left join rfq r on oh.order_id = r.order_id
left join order_products op on oh.order_id = op.order_id
left join customers c on u.user_id = c.user_id
WHERE oh.selling_day is not null
