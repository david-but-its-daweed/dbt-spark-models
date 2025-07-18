{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

WITH raw_requests AS (
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
            WHEN status = 15 THEN 'InProgress'
            WHEN status = 20 THEN 'PriceTooHigh'
            WHEN status = 30 THEN 'ClientNoResponse'
            WHEN status = 40 THEN 'ProductNotFound'
            WHEN status = 50 THEN 'ImpossibleToDeliver'
            WHEN status = 60 THEN 'UnsuitablePartnershipTerms'
            WHEN status = 70 THEN 'Other'
            WHEN status = 80 THEN 'WrongProductLink'
            WHEN status = 90 THEN 'NoAnswerFromMerchant'
            WHEN status = 100 THEN 'RejectedByProcurement'
            WHEN status = 110 THEN 'NeedsCertification'
        END AS request_status
    FROM
        {{ source('mongo', 'b2b_core_customer_requests_daily_snapshot') }}
    WHERE
        link LIKE 'https://joom.pro/pt-br/products/%' OR link LIKE '6%'
),

status AS (
    SELECT DISTINCT
        entityid AS entity_id,
        friendlyid AS deal_friendly_id,
        LAST_VALUE(col.rejectReason) OVER (PARTITION BY entityid, friendlyid ORDER BY TIMESTAMP(col.ctms / 1000) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS status
    FROM
        {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
        LATERAL VIEW
        EXPLODE(statushistory) AS col
),

reject_reasons AS (
    SELECT
        entity_id,
        deal_friendly_id,
        CASE
            WHEN status = 0 THEN 'Empty'
            WHEN status = 1 THEN 'Other'
            WHEN status = 2 THEN 'NotEnoughDataFromMerchant'
            WHEN status = 3 THEN 'UnableToWorkWithMerchant'
            WHEN status = 4 THEN 'ProductIsNotAvailable'
            WHEN status = 5 THEN 'NotEnoughDataForSourcing'
            WHEN status = 6 THEN 'NoResponseFromMerchant'
            WHEN status = 7 THEN 'ProductCannotBeExported'
            WHEN status = 8 THEN 'MinimumOrderQuantity'
            WHEN status = 9 THEN 'PriceTooHigh'
            WHEN status = 10 THEN 'ImpossibleToDeliver'
            WHEN status = 11 THEN 'ProductNotFound'
            WHEN status = 12 THEN 'Test'
            WHEN status = 13 THEN 'NoLegalEntityJoomPro'
            WHEN status = 14 THEN 'NoBankAccountJoomPro'
            WHEN status = 15 THEN 'UnsuitableDeliveryTerms'
            WHEN status = 16 THEN 'UnsuitableFinancialTerms'
            WHEN status = 17 THEN 'UnsuitableSamples'
            WHEN status = 18 THEN 'ClosedWithCompetitor'
            WHEN status = 19 THEN 'Duplicated'
            WHEN status = 20 THEN 'ClientNoResponseAtAll'
            WHEN status = 21 THEN 'ClientNoResponseBeforeDDPQuotation'
            WHEN status = 22 THEN 'ClientNoResponseAfterDDPQuotation'
            WHEN status = 23 THEN 'MarketResearch'
            WHEN status = 24 THEN 'Certification'
            WHEN status = 25 THEN 'LocalMarketPrice'
            WHEN status = 26 THEN 'NeverReply'
            WHEN status = 27 THEN 'PermissionNeeded'
            WHEN status = 28 THEN 'Empty'
            WHEN status = 29 THEN 'OriginalBrandedProduct'
            WHEN status = 30 THEN 'UnderstatedTargetPrice'
            WHEN status = 31 THEN 'LogisticsCost'
            WHEN status = 32 THEN 'CertificationCost'
            WHEN status = 33 THEN 'DoNotMeetTarget'
            WHEN status = 34 THEN 'MoreThanCompetitorOffer'
            WHEN status = 35 THEN 'NoBudget'
            WHEN status = 36 THEN 'FailedToProvideFinancing'
            WHEN status = 37 THEN 'CashFlow'
            WHEN status = 38 THEN 'NeedTimeForDecision'
            WHEN status = 39 THEN 'CanceledByProcurement'
            WHEN status = 40 THEN 'LackOfInfo'
        END AS reject_reason
    FROM
        status
),

requests_with_statuses AS (
    SELECT
        req.deal_id,
        req.request_id,

        req.product_id,
        CASE WHEN req.request_status = 'Other' THEN COALESCE(rej.reject_reason, req.request_status) ELSE req.request_status END AS request_status
    FROM
        raw_requests AS req
    LEFT JOIN
        reject_reasons AS rej
        ON
            req.request_id = rej.entity_id
            OR req.deal_id = rej.entity_id
),

requests AS (
    SELECT
        rs.deal_id,
        cr.customer_request_id AS request_id,
        rs.product_id,
        rs.request_status,
        CAST(cr.expectedQuantity AS INT) AS request_product_qty,

        CAST(cr.expectedQuantity AS INT) * cr.totalPerItem / 1e6 AS total,
        CAST(cr.expectedQuantity AS INT) * cr.sampleDDPPrice AS sample
    FROM {{ source('b2b_mart', 'fact_customer_requests_variants') }} AS cr
    INNER JOIN requests_with_statuses AS rs ON cr.deal_id = rs.deal_id AND cr.customer_request_id = rs.request_id
    WHERE CAST(cr.expectedQuantity AS INT) IS NOT NULL AND CAST(cr.expectedQuantity AS INT) > 0
),

requests_agg AS (
    SELECT
        deal_id,
        request_id,
        product_id,
        request_status,
        SUM(request_product_qty) AS request_product_qty,
        ROUND(SUM(total + CASE WHEN total = 0 AND sample IS NOT NULL THEN sample ELSE 0 END), 2) AS request_ddp
    FROM
        requests
    GROUP BY
        1, 2, 3, 4
),

deals_with_requests AS (
    SELECT
        *,
        SUM(CASE WHEN request_status IN ('PreEstimate', 'InProgress') THEN request_ddp ELSE 0 END) OVER (PARTITION BY deal_id) AS active_deal_ddp,
        COUNT(request_id) OVER (PARTITION BY deal_id) AS requests,
        COUNT(IF(request_status IN ('PreEstimate', 'InProgress'), request_id, NULL)) OVER (PARTITION BY deal_id) AS active_requests
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

        d.request_ddp,
        d.request_product_qty,

        d.active_deal_ddp,

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
    d.deal_status,
    d.deal_status_group,

    md.request_id,
    CASE WHEN md.request_status = 'InProgress' AND d.deal_status = 'DealCompleted' THEN 'Completed' ELSE md.request_status END AS request_status,

    md.merchant_id,
    md.merchant_name,
    md.product_id,
    md.product_name,

    md.request_ddp,
    md.request_product_qty,
    md.active_deal_ddp,

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
