{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

WITH requests_with_statuses AS (
    SELECT
        dealId AS deal_id,
        _id AS request_id,

        CASE
            WHEN link LIKE 'https://joom.pro/pt-br/products/%' THEN REGEXP_EXTRACT(link, 'https://joom.pro/pt-br/products/(.*)', 1)
            ELSE link
        END AS product_id,

        CASE
            WHEN status = 0 THEN 'Unknown'
            WHEN status = 10 THEN 'PreEstimate'
            WHEN status = 15 THEN 'PendingResponses'
            WHEN status = 20 THEN 'PriceTooHigh'
            WHEN status = 30 THEN 'ClientNoResponse'
            WHEN status = 40 THEN 'ProductNotFound'
            WHEN status = 50 THEN 'ImpossibleToDeliver'
            WHEN status = 60 THEN 'UnsuitablePartnershipTerms'
            WHEN status = 70 THEN 'Other'
            WHEN status = 80 THEN 'WrongProductLink'
            WHEN status = 90 THEN 'NoAnswerFromMerchant'
            WHEN status = 100 AND status = 110 THEN 'Cancelled'
        END AS request_status
    FROM
        {{ source('mongo', 'b2b_core_customer_requests_daily_snapshot') }}
    WHERE
        link LIKE 'https://joom.pro/pt-br/products/%' OR link LIKE '6%'
),

requests AS (
    SELECT
        rs.deal_id, 
        cr.customer_request_id AS request_id,
        rs.product_id,
        rs.request_status,
        CAST(expectedQuantity AS INT) AS request_product_qty,
        
        CAST(expectedQuantity AS INT) * totalPerItem / 1e6 AS total,
        CAST(expectedQuantity AS INT) * sampleDDPPrice AS sample
    FROM {{ source('b2b_mart', 'fact_customer_requests_variants') }} AS cr
    JOIN requests_with_statuses AS rs ON cr.deal_id = rs.deal_id AND cr.customer_request_id = rs.request_id
    WHERE CAST(expectedQuantity AS INT) IS NOT NULL AND CAST(expectedQuantity AS INT) > 0
),

requests_agg AS (
    SELECT
        deal_id,
        request_id,
        product_id,
        request_status,
        SUM(request_product_qty) AS request_product_qty,
        ROUND(SUM(total + CASE WHEN total = 0 AND sample IS NOT NULL THEN sample ELSE 0 END), 2) AS ddp_request
    FROM 
        requests
    GROUP BY
        1, 2, 3, 4
),

deals_with_requests AS (
    SELECT
        *,
        SUM(CASE WHEN request_status IN ('PreEstimate', 'PendingResponses') THEN request_product_qty ELSE 0 END) OVER (PARTITION BY deal_id) AS deal_product_qty,
        SUM(CASE WHEN request_status IN ('PreEstimate', 'PendingResponses') THEN ddp_request ELSE 0 END) OVER (PARTITION BY deal_id) AS ddp_deal,
        COUNT(request_id) OVER (PARTITION BY deal_id) AS requests,
        COUNT(IF(request_status IN ('PreEstimate', 'PendingResponses'), request_id, NULL)) OVER (PARTITION BY deal_id) AS active_requests
    FROM
        requests_agg
),

merchant_products AS (
    SELECT DISTINCT
        merchant_id,
        merchant_name,
        product_id,
        product_name
    FROM {{ source('b2b_mart', 'merchant_products') }}
),

merchant_deals AS (
    SELECT
        d.deal_id,
        d.request_id,

        m.product_id,
        m.product_name,

        m.merchant_id,
        m.merchant_name,

        d.request_status,

        d.ddp_request,
        d.request_product_qty,

        d.ddp_deal,
        d.deal_product_qty,

        d.requests,
        d.active_requests
    FROM
        deals_with_requests AS d
    INNER JOIN
        merchant_products AS m ON d.product_id = m.product_id
)

SELECT
    DATE(fcr.created_time) AS partition_date,
    d.deal_friendly_id,
    md.deal_id,
    d.deal_type,
    d.deal_status_group AS deal_status,

    md.request_id,
    md.request_status,

    md.merchant_id,
    md.merchant_name,
    md.product_id,
    md.product_name,

    md.ddp_request,
    md.request_product_qty,
    md.ddp_deal,
    md.deal_product_qty,

    md.requests,
    md.active_requests
FROM
    merchant_deals AS md
INNER JOIN
    {{ source('b2b_mart', 'fact_customer_requests') }} AS fcr
    ON
        md.deal_id = fcr.deal_id
        AND md.request_id = fcr.customer_request_id
        AND md.product_id = fcr.product_id
INNER JOIN
    {{ source('b2b_mart', 'fact_deals_with_requests') }} AS d ON md.deal_id = d.deal_id
WHERE
    fcr.next_effective_ts_msk IS NULL
    AND DATE(fcr.created_time) >= '2025-03-01'