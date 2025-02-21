{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH base_deal AS (
    SELECT
        deal_id,
        user_id,
        deal_name,
        payment_method,
        issue_friendly_id,
        CASE WHEN self_service THEN 1 ELSE 0 END AS self_service,
        CASE WHEN ss_customer THEN 1 ELSE 0 END AS ss_customer,
        status,
        CAST(status_int AS INT) AS status_int,
        CASE
            WHEN reject_reason IS NULL OR status IN (
                'Other', 'MarketResearch', 'PriceTooHigh', 'Duplicated',
                'ClientNoResponseAfterDDPQuotation', 'ClientNoResponseAtAll',
                'Certification', 'ProductNotFound', 'PriceTooHighComparedToCargo',
                'MinimumOrderQuantity', 'Test', 'ClientNoResponseBeforeDDPQuotation',
                'LocalMarketPrice', 'ClosedWithCompetitor', 'UnsuitableFinancialTerms',
                'UnsuitableDeliveryTerms', 'ImpossibleToDeliver'
            ) THEN 'Cancelled'
            ELSE status
        END AS deal_status_group,
        created_ts_msk AS deal_created_ts,
        CAST(created_ts_msk AS DATE) AS deal_created_date,
        order_id,
        owner_ts,
        CAST(owner_ts AS DATE) AS owner_date_msk,
        country,
        estimated_gmv,
        small_batch
    FROM {{ ref('fact_deals') }}
    WHERE CAST(created_ts_msk AS DATE) >= '2024-04-01'
      AND next_effective_ts_msk IS NULL
      AND country = 'BR'
      AND status NOT IN ('Test', 'Duplicated')
),

    base_requests AS (
    SELECT
        customer_request_id,
        deal_id,
        manual,
        standart_deal,
        rfq_deal,
        sample,
        category_name
    FROM {{ ref('fact_customer_requests') }}
    WHERE next_effective_ts_msk IS NULL
),

    requests_variants AS (
    SELECT
        customer_request_id,
        ROW_NUMBER() OVER (PARTITION BY customer_request_id) AS variant_num,
        CAST(expectedQuantity AS INT) AS qty,
        COUNT(
            CASE
                WHEN expectedQuantity IS NOT NULL AND CAST(expectedQuantity AS INT) > 0
                THEN sub_product_id
            END
        ) OVER (PARTITION BY customer_request_id) AS variantsCart,
        sample_type,
        ddpPerItem / 1000000 AS ddpPerItem,
        exwPerItem / 1000000 AS exwPerItem,
        taxBasePerItem / 1000000 AS taxBasePerItem,
        totalPerItem / 1000000 AS totalPerItem,
        CAST(expectedQuantity AS INT) * ddpPerItem / 1000000 AS ddp,
        CAST(expectedQuantity AS INT) * exwPerItem / 1000000 AS exw,
        CAST(expectedQuantity AS INT) * taxBasePerItem / 1000000 AS taxBase,
        CAST(expectedQuantity AS INT) * totalPerItem / 1000000 AS total,
        CAST(expectedQuantity AS INT) * sampleDDPPrice / 1000000 AS sample_ddp,
        merchant_price_per_item / 1000000 AS merchant_price_per_item,
        merchant_price_total_amount / 1000000 AS merchant_price_total_amount
    FROM {{ ref('fact_customer_requests_variants') }}
),

    wide_table AS (
    SELECT DISTINCT *
    FROM base_deal
    LEFT JOIN base_requests USING (deal_id)
    LEFT JOIN requests_variants USING (customer_request_id)
),

    deals_agg_stat AS (
    SELECT m.*,
           products,
           current_status_date
    FROM (
        SELECT
            deal_id,
            issue_friendly_id AS deal_friendly_id,
            user_id,
            deal_name,
            payment_method,
            estimated_gmv,
            self_service,
            ss_customer,
            deal_status_group,
            status AS deal_status,
            status_int AS deal_status_int,
            deal_created_ts,
            deal_created_date,
            order_id,
            owner_ts,
            owner_date_msk,
            COUNT(DISTINCT customer_request_id) AS count_customer_requests,
            COUNT(customer_request_id) AS count_customer_requests_variants,
            SUM(qty) AS qty,
            ROUND(SUM(taxBase + CASE WHEN payment_method = 'deferred' THEN exw / 105 ELSE 0 END), 2) AS other,
            ROUND(SUM(CASE WHEN payment_method = 'deferred' THEN exw * 100 / 105 ELSE exw END), 2) AS exw,
            ROUND(SUM(total + CASE WHEN total = 0 AND sample_ddp IS NOT NULL THEN sample_ddp ELSE 0 END), 2) AS ddp,
            MAX(CASE WHEN sample_type = 0 THEN 1 ELSE 0 END) AS with_onlineReview,
            MAX(CASE WHEN sample_type = 1 THEN 1 ELSE 0 END) AS with_sampleDelivery,
            MAX(CASE WHEN standart_deal OR (rfq_deal IS FALSE AND sample IS FALSE AND manual IS FALSE) THEN 1 ELSE 0 END) AS is_standart,
            MAX(CASE WHEN rfq_deal THEN 1 ELSE 0 END) AS is_rfq,
            MAX(CASE WHEN sample THEN 1 ELSE 0 END) AS is_sample,
            MAX(CASE WHEN manual THEN 1 ELSE 0 END) AS is_manual,
            MAX(CASE WHEN small_batch THEN 1 ELSE 0 END) AS is_small_batch
        FROM wide_table
        GROUP BY ALL
    ) AS m
    LEFT JOIN (
        SELECT deal_id,
               COUNT(product_id) AS products
        FROM {{ ref('dim_deal_products') }}
        GROUP BY deal_id
    ) USING (deal_id)
    LEFT JOIN (
        SELECT deal_id,
               current_status_date
        FROM {{ ref('fact_deals_statuses') }}
    ) USING (deal_id)
),

    base_order AS (
    SELECT
        order_id,
        CAST(created_ts_msk AS DATE) AS order_created_date,
        friendly_id AS order_friendly_id,
        current_status AS order_current_status
    FROM {{ ref('fact_order') }}
    JOIN (
        SELECT order_id, current_status
        FROM {{ ref('fact_order_statuses') }}
    ) st USING (order_id)
    WHERE next_effective_ts_msk IS NULL
),

    order_data AS (
    SELECT *
    FROM (
        SELECT
            order_id,
            order_created_date,
            order_friendly_id,
            order_current_status,
            total_confirmed_price,
            final_gross_profit,
            initial_gross_profit,
            owner_moderator_id,
            final_gmv,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts_msk DESC) AS row_n
        FROM {{ ref('fact_order_change') }}
        JOIN base_order USING (order_id)
    )
    WHERE row_n = 1
),

    marketing_deals_interactions AS (
    SELECT user_id,
           deal_id,
           country,
           visit_ts_msk,
           visit_date,
           utm_campaign,
           utm_source,
           utm_medium,
           CASE WHEN type = 'organic' THEN 'Unrecognised_deal_source' ELSE source END AS source,
           CASE WHEN type = 'organic' THEN 'Unrecognised_type' ELSE type END AS type,
           first_utm_campaign,
           first_utm_sourceas,
           first_utm_medium,
           first_source,
           first_type,
           number_of_interactions
    FROM {{ ref('fact_marketing_deals_interactions') }}
)


SELECT d.deal_id,
       d.deal_friendly_id,
       d.user_id,
       d.deal_name,
       d.payment_method,
       d.estimated_gmv,
       d.self_service,
       d.ss_customer,
       d.deal_status_group,
       d.deal_status,
       d.deal_status_int,
       d.deal_created_ts,
       d.deal_created_date,
       d.order_id,
       d.owner_ts,
       d.owner_date_msk,
       d.count_customer_requests,
       d.count_customer_requests_variants,
       d.qty,
       d.other,
       d.exw,
       d.ddp,
       d.with_onlineReview,
       d.with_sampleDelivery,
       d.is_standart,
       d.is_rfq,
       d.is_sample,
       d.is_manual,
       d.is_small_batch,
       d.products,
       d.current_status_date,
       o.order_created_date,
       o.order_friendly_id,
       o.order_current_status,
       o.total_confirmed_price,
       o.final_gross_profit,
       o.initial_gross_profit,
       o.owner_moderator_id,
       o.final_gmv,
       utm_campaign,
       utm_source,
       utm_medium,
       source,
       type,
       first_utm_campaign,
       first_utm_medium,
       first_source,
       first_type,
       number_of_interactions AS count_visits,
       ROW_NUMBER() OVER (PARTITION BY d.user_id ORDER BY deal_created_ts) AS number_user_deal
FROM deals_agg_stat AS d
LEFT JOIN order_data AS o ON d.order_id = o.order_id
LEFT JOIN marketing_deals_interactions AS di ON d.deal_id = di.deal_id
