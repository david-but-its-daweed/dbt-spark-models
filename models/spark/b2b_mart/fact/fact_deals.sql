{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH key_status AS (
    SELECT
        id,
        status
    FROM {{ ref('key_issue_status') }}
),

owner AS (
    SELECT
        fi.entity_id AS deal_id,
        fi.issue_friendly_id,
        fi.assignee_id AS owner_id,
        fi.assignee_email AS owner_email,
        fi.assignee_role AS owner_role,
        fi.first_time_assigned AS owner_ts,
        fi.status_time,
        ---case when lower(fi.status) = 'delivering' then 'Delivery' else fi.status end as status,
        fi.status,
        ks.id as status_int, 
        fi.reject_id,
        fi.reject_reason,
        fi.reject_reason_comment
    FROM {{ ref('fact_issues') }} AS fi
    LEFT JOIN key_status AS ks ON fi.status = ks.status
    WHERE type like '%CloseTheDeal%' AND next_effective_ts_msk IS NULL
),

purchase AS (
    SELECT *
    FROM
    (
    SELECT
        entity_id AS deal_id,
        assignee_id AS purchaser_id,
        assignee_email AS purchaser_email,
        assignee_role AS purchaser_role,
        row_number() over (partition by entity_id order by last_time_assigned desc) as rn
    FROM {{ ref('fact_issues') }}
    WHERE type = 'DealPurchaseSupport'
    AND next_effective_ts_msk IS NULL
    )
    WHERE rn = 1
),


source AS
(
    SELECT 
        user_id,
        source,
        type,
        campaign,
        min_date_payed
    FROM {{ ref('fact_attribution_interaction') }}
    WHERE last_interaction_type
),
 
 
users AS (
    SELECT DISTINCT
        user_id,
        grade,
        company_name,
        country,
        cnpj != "" as ss_customer
    FROM {{ ref('fact_customers') }}
),

paymentmethod AS (
    SELECT *
    FROM (
        SELECT deal_id,
               payment_method,
               delivery_channel_id, 
               case 
                   when delivery_channel_id = 9 then 'Aero Br'
                   when delivery_channel_id = 8 then 'Sea Br' 
               end as delivery_channel_type,
               ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY created_ts_msk) AS row_n
        FROM {{ ref('scd2_calculations_snapshot') }}
        WHERE deal_id IS NOT NULL
          AND dbt_valid_to IS NULL
    ) AS main
    WHERE row_n = 1
),
    
friendly_statuses AS (
    SELECT lower(replace(key,'issue[dot]status[dot]','')) AS key_j,
           MAX(
               CASE
                   WHEN lower(replace(key,'issue[dot]status[dot]','')) = 'delivering' THEN 'Delivery'
                   ELSE val
               END
           ) AS friendly_status
    FROM {{ source('mongo', 'b2b_core_i18ndata_daily_snapshot') }}
    WHERE langCode = 'en'
      AND key LIKE '%issue[dot]status[dot]%'
    GROUP BY 1
)


SELECT DISTINCT
    d._id AS deal_id,
    d.userId as user_id,
    d.contractId AS contract_id,
    d.country,
    millis_to_ts_msk(d.ctms) AS created_ts_msk,
    d.update_ts_msk AS updated_ts_msk,
    d.description AS deal_description,
    d.docsFolderId AS docs_folder_id,
    millis_to_ts_msk(d.estimatedEndDate) AS estimated_date,
    d.estimatedGmv.amount/1000000 AS estimated_gmv,
    d.estimatedGmv.ccy AS estimated_gmv_ccy,
    d.interactionId AS interaction_id,
    CASE 
        WHEN d.legalScheme = 0 THEN 'Broker'
        WHEN d.legalScheme = 0 THEN 'Agent' 
        END AS legal_scheme,
    d.name AS deal_name, 
    d.orderId AS order_id,
    d.payment.advancePercent/10000 AS advance_percent,
    d.payment.clientCurrency AS client_currency,
    d.payment.completePaymentAfter AS complete_payment_after,
    CASE 
        WHEN payment.paymentType = 1 THEN 'advance'
        WHEN payment.paymentType = 2 THEN 'complete'
        END AS payment_type,
    d.payment.paymentWithinDaysAdvance AS payment_within_days_advance,
    d.payment.paymentWithinDaysComplete AS payment_within_days_complete,
    CASE
        WHEN d.payment.workScheme = 0 THEN 'SIA'
        WHEN d.payment.workScheme = 1 THEN 'InternetProject'
        END AS work_scheme,
    CASE
        WHEN d.requestType = 1 THEN 'standard'
        WHEN d.requestType = 2 THEN 'vip'
        END AS deal_type,
    CASE
        WHEN d.sourcingDealType = 0 THEN 'standard'
        WHEN d.sourcingDealType = 1 THEN 'vip'
    END AS sourcing_deal_type,
    owner.issue_friendly_id,
    owner.owner_id,
    owner.owner_email,
    owner.owner_role,
    owner.owner_ts,
    owner.status_time,
    case when lower(owner.status) = 'delivering' then 'Delivery' else owner.status end as status,
    owner.status_int,
    friendly_statuses.friendly_status,
    owner.reject_id,
    owner.reject_reason,
    owner.reject_reason_comment,
    purchase.purchaser_id,
    purchase.purchaser_email,
    purchase.purchaser_role,
    source.source,
    source.type,
    source.campaign,
    source.min_date_payed,
    CASE
        WHEN millis_to_ts_msk(d.ctms) >= source.min_date_payed AND source.min_date_payed IS NOT NULL THEN TRUE
        ELSE FALSE END AS retention,
    users.grade AS client_grade,
    users.company_name AS client_company_name,
    d.isSelfService AS self_service,
    d.isSmallBatch as small_batch,
    users.ss_customer,
    paymentmethod.payment_method,
    paymentmethod.delivery_channel_type,
    TIMESTAMP(d.dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(d.dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_deals_snapshot') }} AS d
LEFT JOIN owner ON d._id = owner.deal_id
LEFT JOIN purchase ON d._id = purchase.deal_id
LEFT JOIN source ON source.user_id = d.userId
LEFT JOIN users ON users.user_id = d.userId
LEFT JOIN paymentmethod ON d._id = paymentmethod.deal_id
LEFT JOIN friendly_statuses on friendly_statuses.key_j = lower(owner.status) 
