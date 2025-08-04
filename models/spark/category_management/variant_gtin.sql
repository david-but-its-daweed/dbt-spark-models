{{
  config(
    materialized='incremental',
    alias='variant_gtin',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    },
    on_schema_change='append_new_columns'
  )
}}

WITH base AS (
    SELECT
        dd.id AS dt,
        pv.variant_id,
        pv.product_id,
        pv.merchant_id,
        pv.merchant_name,
        pv.gtin AS variant_gtin,
        pc.brand_class,
        pc.brand_name,
        pc.is_public AS is_product_public,
        pc.archived AS is_product_archived,
        pc.removed AS is_product_removed,
        pc.gtin AS product_gtin,
        gp.l1_merchant_category_id,
        gp.l1_merchant_category_name,
        gp.l2_merchant_category_id,
        gp.l2_merchant_category_name,
        gp.business_line,
        pv.merchant_origin_name AS origin_name,
        CASE
            WHEN pv.merchant_origin_name = 'Japanese' THEN 'a.titova@joom.com'
            WHEN pv.merchant_origin_name = 'Korean' THEN 'ozerova@joom.com'
            WHEN pv.merchant_origin_name = 'Indian' THEN 'vladimir.reznichenko@joom.com'
            WHEN pv.merchant_origin_name IN (
                'Irish', 'British', 'Spanish', 'Romanian', 'Lithuanian', 'Bulgarian',
                'Latvian', 'Polish', 'Italian', 'French', 'Estonian', 'Greek', 'Czech',
                'Ukrainian', 'Austrian', 'German', 'Portuguese', 'Luxembourgish', 'Slovakian',
                'Dutch', 'Slovenian', 'Swedish', 'Danish', 'Cypriot', 'Hungarian', 'Croatian', 'Belgian'
            ) THEN 'kostenkova@joom.com'
            ELSE mkm.kam_email
        END AS kam_email,
        COALESCE(SUM(IF(DATE(go.order_datetime_utc) = dd.id, go.gmv_initial, 0))) AS gmv_initial_sum,
        COALESCE(SUM(IF(DATE(go.order_datetime_utc) = dd.id, go.order_gross_profit_final_estimated, 0))) AS gp_final_estimated_sum,
        COALESCE(SUM(go.gmv_initial), 0) AS gmv_initial_sum30,
        COALESCE(SUM(go.order_gross_profit_final_estimated), 0) AS gp_final_estimated_sum30
    FROM {{ source('mart','dim_date') }} AS dd -- mart.dim_date as dd
    INNER JOIN {{ source('mart','dim_published_variant_with_merchant') }} AS pv -- mart.dim_published_variant_with_merchant as pv
        ON
            1 = 1
            AND TIMESTAMP(dd.id) BETWEEN pv.effective_ts AND pv.next_effective_ts
            AND pv.public IS true -- отбираем только публичные варианты на каждый день
    INNER JOIN {{ source('mart','published_products_current') }} AS pc -- mart.published_products_current as pc
        ON
            1 = 1
            AND pv.product_id = pc.product_id
    INNER JOIN {{ ref('gold_products') }} AS gp -- gold.products as gp
        ON
            1 = 1
            AND pc.product_id = gp.product_id
    LEFT JOIN {{ source('category_management', 'merchant_kam_materialized') }} AS mkm --category_management.merchant_kam_materialized as mkm
        ON
            1 = 1
            AND pv.merchant_id = mkm.merchant_id
            AND mkm.quarter = DATE_TRUNC('quarter', dd.id)
    LEFT JOIN {{ ref('gold_orders') }} AS go -- gold.orders as go
        ON
            1 = 1
            AND pv.variant_id = go.product_variant_id
            AND DATE(go.order_datetime_utc) BETWEEN (dd.id - 29) AND dd.id
    WHERE
        1 = 1
        --and dd.id between '2025-06-01' and (current_date - 1)
        {% if is_incremental() %}
            AND dd.id = DATE '{{ var("start_date_ymd") }}'
        {% else %}
            AND dd.id BETWEEN '2025-07-01' AND (DATE '{{ var("start_date_ymd") }}')
        {% endif %}
    GROUP BY
        dd.id,
        pv.variant_id,
        pv.product_id,
        pv.merchant_id,
        pv.merchant_name,
        pv.gtin,
        pc.brand_class,
        pc.brand_name,
        pc.is_public,
        pc.archived,
        pc.removed,
        pc.gtin,
        gp.l1_merchant_category_id,
        gp.l1_merchant_category_name,
        gp.l2_merchant_category_id,
        gp.l2_merchant_category_name,
        gp.business_line,
        pv.merchant_origin_name,
        mkm.kam_email
),

docs AS (
    SELECT
        l.productid AS product_id,
        --
        COUNT_IF(d.status = 2 AND l.status = 1) > 0 AS has_active_document--,
        --
        --count_if(d.status=1 and l.status=1)>0 as has_rejected_document,
        --count_if(d.status=3 and l.status=1)>0 as has_expired_document,
        --count_if(d.status=4 and l.status=1)>0 as has_document_act_req,
        --count_if(d.status=9 and l.status=1)>0 as has_deactivated_document,
        --
        --count_if(d.status=2 and l.status=0)>0 as has_pending_link,
        --count_if(d.status=2 and l.status=2)>0 as has_rejeted_link,
        --count_if(d.status=2 and l.status=3)>0 as has_warning_link,
        --
        --count_if(d.status=4)>0 as has_document_act_req_any_link,
        --max(d.effectiveenddate) as document_eff_to
    FROM {{ source('mongo', 'core_merchant_documents_daily_snapshot') }} AS d -- mongo.core_merchant_documents_daily_snapshot as d
    LEFT JOIN {{ source('mongo', 'product_product_document_links_daily_snapshot') }} AS l -- mongo.product_product_document_links_daily_snapshot as l 
        ON
            1 = 1
            AND d._id = l.documentid
    WHERE
        1 = 1
        AND d.type = 20 -- EAR
    GROUP BY
        l.productid
)

SELECT
    b.dt AS partition_date,
    b.variant_id,
    b.product_id,
    b.merchant_id,
    b.merchant_name,
    b.variant_gtin,
    b.brand_class,
    b.brand_name,
    b.is_product_public,
    b.is_product_archived,
    b.is_product_removed,
    b.product_gtin,
    b.l1_merchant_category_id,
    b.l1_merchant_category_name,
    b.l2_merchant_category_id,
    b.l2_merchant_category_name,
    b.business_line,
    b.origin_name,
    b.kam_email,
    b.gmv_initial_sum,
    b.gp_final_estimated_sum,
    b.gmv_initial_sum30,
    b.gp_final_estimated_sum30,
    d.has_active_document AS has_active_ear
FROM base AS b
LEFT JOIN docs AS d
    ON
        1 = 1
        AND b.product_id = d.product_id