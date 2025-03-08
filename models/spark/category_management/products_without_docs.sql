{{
  config(
    materialized='table',
    alias='products_without_docs',
    file_format='parquet',
    schema='category_management',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    },
  )
}}

WITH docs_stg AS (
    SELECT
        doc_id,
        merchantid,
        name,
        is_packaging,
        CASE                   --- Статус
            WHEN status = 0 THEN "unverified"
            WHEN status = 1 THEN "rejected"
            WHEN status = 2 THEN "active"
            WHEN status = 3 THEN "expired"
            WHEN status = 9 THEN "deactivated"
            ELSE status
        END AS status,
        CASE                  --- Тип документа  
            WHEN type = 1 THEN "trademarkDistributor"
            WHEN type = 2 THEN "trademarkOwner"
            WHEN type = 3 THEN "trademarkReseller"
            WHEN type = 4 THEN "vat"
            WHEN type = 5 THEN "vatDeclaration"
            WHEN type = 6 THEN "fda"
            WHEN type = 7 THEN "ec" -- нужно в первую волну
            WHEN type = 8 THEN "msds"
            WHEN type = 9 THEN "warehouseService"
            WHEN type = 10 THEN "warehouseResale"
            WHEN type = 11 THEN "doc" -- нужно в первую волну
            WHEN type = 12 THEN "productInstruction"
            WHEN type = 13 THEN "eprGermany" -- нужно в первую волну
            WHEN type = 14 THEN "eprFrance" -- нужно в первую волну
            WHEN type = 15 THEN "eprAustria" -- нужно в первую волну
            WHEN type = 16 THEN "eprGermanyElectronics" -- нужно в первую волну
            WHEN type = 17 THEN "eprFinland"
            WHEN type = 18 THEN "eprSweden"
            WHEN type = 19 THEN "eprSpain"
            WHEN type = 20 THEN "ear"
            ELSE type
        END AS type,
        IF(attachments IS NULL, NULL, attachments.enddate) AS end_date,
        IF(attachments IS NULL, NULL, attachments.startdate) AS start_date
    FROM (
        SELECT
            _id AS doc_id,         --- doc_id,
            merchantid,            --- Идентификатор мерчанта
            name,                  --- Название документа
            payload.productkind = 42 AS is_packaging, --- Нужно для eprGermany, eprFrance, eprAustria 
            status,
            type,
            EXPLODE(attachments) AS attachments
        FROM {{ source('mongo', 'core_merchant_documents_daily_snapshot') }}

        UNION ALL

        SELECT
            _id AS doc_id,         --- doc_id,
            merchantid,            --- Идентификатор мерчанта
            name,                  --- Название документа
            payload.productkind = 42 AS is_packaging, --- Нужно для eprGermany, eprFrance, eprAustria 
            status,
            type,
            NULL AS attachments
        FROM {{ source('mongo', 'core_merchant_documents_daily_snapshot') }}
        WHERE ARRAY_SIZE(attachments) = 0
    )
),

docs AS (
    SELECT
        doc_id,
        merchantid,
        name,
        status,
        type,
        is_packaging,
        MIN(TO_DATE(end_date, "yyyyMMdd")) AS end_date,
        MAX(TO_DATE(start_date, "yyyyMMdd")) AS start_date
    FROM docs_stg
    GROUP BY 1, 2, 3, 4, 5, 6
),

--  старая логика через productKind'ы
links AS (
    SELECT
        _id AS product_id,
        EXPLODE(docids) AS doc_id
    FROM {{ source('mongo', 'product_merchant_document_product_links_daily_snapshot') }}
),

-- новая логика через связку - документ
links_v2 AS (
    SELECT
        pp.productid AS product_id,
        pp.documentid AS doc_id
    FROM {{ source('mongo', 'product_product_document_links_daily_snapshot') }} AS pp
    --    JOIN {{ source('mongo', 'core_merchant_documents_daily_snapshot') }} AS cm ON cm._id=pp.documentid
    WHERE pp.status = 1 -- статус связки StatusVerified
    --   AND cm.type in (6, 7, 11) 
    --   AND cm.status=2 -- статус документа StatusActive
),

products AS (
    SELECT
        p.product_id,
        p.merchant_category_id,
        p.merchant_id,
        m.origin_name
    FROM {{ ref('gold_products') }} AS p
    INNER JOIN {{ ref('gold_merchants') }} AS m ON p.merchant_id = m.merchant_id
    WHERE p.is_public = TRUE
),
----------------------------------------------------
-----------------Only available in EU products------
----------------------------------------------------

countries AS (
    SELECT country_code
    FROM {{ ref('gold_countries') }}
    WHERE region_name = "Europe"
),

prod_ext AS (
    SELECT
        pc.product_id,
        pc.merchant_category_id,
        pc.merchant_id,
        pc.origin_name
    FROM (
        SELECT
            p.product_id,
            p.merchant_category_id,
            p.merchant_id,
            p.origin_name,
            pa.country AS availability_country
        FROM products AS p
        INNER JOIN {{ source('ads', 'product_availability_v2') }} AS pa ON p.product_id = pa.product_id
        WHERE pa.partition_date >= CURRENT_DATE() - 7
    ) AS pc
    INNER JOIN countries AS c ON pc.availability_country = c.country_code
    GROUP BY 1, 2, 3, 4
),
----------------------------------------------------
---Define categories for eprgermanyelectronics------
----------------------------------------------------

categ_eprgermanyelectronics AS (
    SELECT merchant_category_id AS col
    FROM {{ ref('gold_merchant_categories') }}
    WHERE merchant_category_id IN (
        "1473502939429845522-112-2-118-2549112422", "1565185374381325215-6-2-719-2655932728", "1538113739386104948-8-2-709-2417277231",
        "1473502937542431302-254-2-118-775563897", "1473502934428710625-56-2-118-1077948298", "1473502940553286951-222-2-118-1051900831",
        "1534143952219038240-224-2-39131-1542348759", "1473502942438525573-43-2-118-2592026259", "1473502945380600110-142-2-118-3931277175",
        "1473502934439215963-59-2-118-2107978403", "1473502934470784530-67-2-118-69793029", "1669796796622311300-172-2-45574-4221961603",
        "1669983767114497897-27-2-25735-2958458682", "1473502935756243552-193-2-118-2286045444", "1473502934759284889-152-2-118-2768240065",
        "1473502935078574724-250-2-118-1809995431", "1473502944806831882-215-2-118-3959005051", "1473502944865996052-233-2-118-949330424",
        "1473502944884898332-239-2-118-1858289495", "1473502944938146782-1-2-118-2149260978", "1473502945031480796-31-2-118-3435466436",
        "1473502945067250617-43-2-118-1413317048", "1473502946209524780-160-2-118-871081881", "1645015524034977160-211-2-43019-858789387",
        "1473502939762448606-223-2-118-1695232020", "1473502936628585095-202-2-118-2334646893", "1473502939676948853-192-2-118-3954613129",
        "1473502939669657057-190-2-118-459536113", "1473502937340235814-180-2-118-178808103", "1540456934635660269-89-2-39116-2249646085",
        "1566977877483889611-197-2-26202-3224025977", "1473502942275811681-255-2-118-2896216843", "1671096642056364100-127-2-51462-3508518140",
        "1473502934611440722-108-2-118-1516332294", "1473502934617296186-110-2-118-1385317188", "1473502934622717250-112-2-118-108994840",
        "1473502945383641600-143-2-118-269455737", "1473502935496536265-115-2-118-4291197627", "1473502935501996981-117-2-118-1521365433",
        "1496396987107433838-131-2-709-3432694697", "1482946851095071775-165-2-26312-2782726624", "1562579111252670336-182-2-39044-4223979409"
    )
),

categories_ege AS (
    SELECT
        c.merchant_category_id,
        c.merchant_category_name
    FROM {{ ref('gold_merchant_categories') }} AS c
    LEFT JOIN categ_eprgermanyelectronics AS c5 ON c.l5_merchant_category_id = c5.col
    LEFT JOIN categ_eprgermanyelectronics AS c4 ON c.l4_merchant_category_id = c4.col
    LEFT JOIN categ_eprgermanyelectronics AS c3 ON c.l3_merchant_category_id = c3.col
    LEFT JOIN categ_eprgermanyelectronics AS c2 ON c.l2_merchant_category_id = c2.col
    LEFT JOIN categ_eprgermanyelectronics AS c1 ON c.l1_merchant_category_id = c1.col
    WHERE
        c5.col IS NOT NULL
        OR c4.col IS NOT NULL
        OR c3.col IS NOT NULL
        OR c2.col IS NOT NULL
        OR c1.col IS NOT NULL

),
----------------------------------------------------
---------Define categories for ce, fda, doc---------
----------------------------------------------------

categ_ce_fda_pi_doc AS (
    SELECT c.merchant_category_id AS col
    FROM {{ ref('gold_merchant_categories') }} AS c
    INNER JOIN {{ source('category_management', 'categories_with_ce_requirements') }} AS cc ON c.merchant_category_id = cc.category_id
),

categories_ce_fda_pi_doc_full AS (
    SELECT
        c.merchant_category_id,
        c.merchant_category_name
    FROM {{ ref('gold_merchant_categories') }} AS c
    LEFT JOIN categ_ce_fda_pi_doc AS c5 ON c.l5_merchant_category_id = c5.col
    LEFT JOIN categ_ce_fda_pi_doc AS c4 ON c.l4_merchant_category_id = c4.col
    LEFT JOIN categ_ce_fda_pi_doc AS c3 ON c.l3_merchant_category_id = c3.col
    LEFT JOIN categ_ce_fda_pi_doc AS c2 ON c.l2_merchant_category_id = c2.col
    LEFT JOIN categ_ce_fda_pi_doc AS c1 ON c.l1_merchant_category_id = c1.col
    WHERE
        c5.col IS NOT NULL
        OR c4.col IS NOT NULL
        OR c3.col IS NOT NULL
        OR c2.col IS NOT NULL
        OR c1.col IS NOT NULL

),

----------------------------------------------------
-------------------GMV calculatios------------------
----------------------------------------------------
gmv AS (
    SELECT
        product_id,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 30, gmv_initial, 0)) AS gmv_30_day_all,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90, gmv_initial, 0)) AS gmv_90_day_all,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180, gmv_initial, 0)) AS gmv_180_day_all,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND country_code = "FR", gmv_initial, 0)) AS gmv_30_day_fr,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND country_code = "FR", gmv_initial, 0)) AS gmv_90_day_fr,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND country_code = "FR", gmv_initial, 0)) AS gmv_180_day_fr,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND country_code = "DE", gmv_initial, 0)) AS gmv_30_day_de,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND country_code = "DE", gmv_initial, 0)) AS gmv_90_day_de,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND country_code = "DE", gmv_initial, 0)) AS gmv_180_day_de,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND country_code = "AT", gmv_initial, 0)) AS gmv_30_day_at,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND country_code = "AT", gmv_initial, 0)) AS gmv_90_day_at,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND country_code = "AT", gmv_initial, 0)) AS gmv_180_day_at,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND country_code = "ES", gmv_initial, 0)) AS gmv_30_day_es,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND country_code = "ES", gmv_initial, 0)) AS gmv_90_day_es,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND country_code = "ES", gmv_initial, 0)) AS gmv_180_day_es,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND country_code = "SE", gmv_initial, 0)) AS gmv_30_day_se,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND country_code = "SE", gmv_initial, 0)) AS gmv_90_day_se,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND country_code = "SE", gmv_initial, 0)) AS gmv_180_day_se,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND country_code = "FI", gmv_initial, 0)) AS gmv_30_day_fi,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND country_code = "FI", gmv_initial, 0)) AS gmv_90_day_fi,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND country_code = "FI", gmv_initial, 0)) AS gmv_180_day_fi,

        SUM(IF(order_date_msk >= CURRENT_DATE() - 30 AND region_name = "Europe", gmv_initial, 0)) AS gmv_30_day_eu,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 90 AND region_name = "Europe", gmv_initial, 0)) AS gmv_90_day_eu,
        SUM(IF(order_date_msk >= CURRENT_DATE() - 180 AND region_name = "Europe", gmv_initial, 0)) AS gmv_180_day_eu
    FROM {{ ref('gold_orders') }}
    WHERE order_date_msk >= CURRENT_DATE() - 365
    GROUP BY 1
),

agg AS (
    SELECT
        p.product_id,
        p.merchant_id,
        p.origin_name,
        p.merchant_category_id,
        gc.merchant_category_name,
        gc.l1_merchant_category_name,
        gc.l2_merchant_category_name,
        gc.l3_merchant_category_name,

        MAX(COALESCE(d.type = "ec" AND d.status = "active" AND cc.merchant_category_id IS NOT NULL, FALSE)) AS has_active_ec,
        MIN(IF(d.type = "ec" AND cc.merchant_category_id IS NOT NULL, d.end_date, NULL)) AS end_date_ec,
        MAX(IF(d.type = "ec" AND cc.merchant_category_id IS NOT NULL, d.start_date, NULL)) AS start_date_ec,

        MAX(COALESCE(d.type = "doc" AND d.status = "active" AND cc.merchant_category_id IS NOT NULL, FALSE)) AS has_active_doc,
        MIN(IF(d.type = "doc" AND cc.merchant_category_id IS NOT NULL, d.end_date, NULL)) AS end_date_doc,
        MAX(IF(d.type = "doc" AND cc.merchant_category_id IS NOT NULL, d.start_date, NULL)) AS start_date_doc,

        MAX(COALESCE(dv.type = "ec" AND dv.status = "active", FALSE)) AS has_active_ec_v2,
        MIN(IF(dv.type = "ec" AND dv.status = "active", dv.end_date, NULL)) AS end_date_ec_v2,
        MAX(IF(dv.type = "ec" AND dv.status = "active", dv.start_date, NULL)) AS start_date_ec_v2,

        MAX(COALESCE(dv.type = "doc" AND dv.status = "active", FALSE)) AS has_active_doc_v2,
        MIN(IF(dv.type = "doc" AND dv.status = "active", dv.end_date, NULL)) AS end_date_doc_v2,
        MAX(IF(dv.type = "doc" AND dv.status = "active", dv.start_date, NULL)) AS start_date_doc_v2,

        MAX(COALESCE(dv.type = "fda" AND dv.status = "active", FALSE)) AS has_active_fda_v2,
        MIN(IF(dv.type = "fda" AND dv.status = "active", dv.end_date, NULL)) AS end_date_fda_v2,
        MAX(IF(dv.type = "fda" AND dv.status = "active", dv.start_date, NULL)) AS start_date_fda_v2,

        MAX(COALESCE(d.type = "eprGermanyElectronics" AND d.status = "active" AND c.merchant_category_id IS NOT NULL, FALSE)) AS has_active_eprgermanyelectronics,
        MIN(IF(d.type = "eprGermanyElectronics" AND c.merchant_category_id IS NOT NULL, d.end_date, NULL)) AS end_date_eprgermanyrlectronics,
        MAX(IF(d.type = "eprGermanyElectronics" AND c.merchant_category_id IS NOT NULL, d.start_date, NULL)) AS start_date_eprgermanyelectronics,

        MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprGermany" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprGermany" AND d.status = "active", FALSE)
            END
        ) AS has_active_eprgermany,
        MIN(IF(dm.type = "eprGermany" AND dm.status = "active", dm.end_date, NULL)) AS end_date_eprgermany,
        MAX(IF(dm.type = "eprGermany" AND dm.status = "active", dm.start_date, NULL)) AS start_date_eprgermany,

        MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprFrance" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = TRUE THEN COALESCE(d.type = "eprFrance" AND d.status = "active", FALSE)
            END
        ) AS has_active_eprfrance,
        MIN(IF(dm.type = "eprFrance" AND dm.status = "active", dm.end_date, NULL)) AS end_date_eprfrance,
        MAX(IF(dm.type = "eprFrance" AND dm.status = "active", dm.start_date, NULL)) AS start_date_eprfrance,

        MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprAustria" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprAustria" AND d.status = "active", FALSE)
            END
        ) AS has_active_epraustria,
        MIN(IF(dm.type = "eprAustria" AND dm.status = "active", dm.end_date, NULL)) AS end_date_epraustria,
        MAX(IF(dm.type = "eprAustria" AND dm.status = "active", dm.start_date, NULL)) AS start_date_epraustria,

        MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprFinland" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprFinland" AND d.status = "active", FALSE)
            END
        ) AS has_active_eprfinland,
        MIN(IF(dm.type = "eprFinland" AND dm.status = "active", dm.end_date, NULL)) AS end_date_eprfinland,
        MAX(IF(dm.type = "eprFinland" AND dm.status = "active", dm.start_date, NULL)) AS start_date_eprfinland,

        MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprSweden" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprSweden" AND d.status = "active", FALSE)
            END
        ) AS has_active_eprsweden,
        MIN(IF(dm.type = "eprSweden" AND dm.status = "active", dm.end_date, NULL)) AS end_date_eprsweden,
        MAX(IF(dm.type = "eprSweden" AND dm.status = "active", dm.start_date, NULL)) AS start_date_eprsweden,

        MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprSpain" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprSpain" AND d.status = "active", FALSE)
            END
        ) AS has_active_eprspain,
        MIN(IF(dm.type = "eprSpain" AND dm.status = "active", dm.end_date, NULL)) AS end_date_eprspain,
        MAX(IF(dm.type = "eprSpain" AND dm.status = "active", dm.start_date, NULL)) AS start_date_eprspain,

        MAX(COALESCE(dm.type = "ear" AND dm.status = "active", FALSE)) AS has_active_ear

    FROM prod_ext AS p
    ----Для  eprGermany, eprFrance, eprAustria документ относится ко всем товарам продавца, если productKind - packaging
    ----К документу можно привязать товары и он будет относится только к ним, если выбраны другие productKind
    ----Для остальных связь по линку
    LEFT JOIN docs AS dm ON p.merchant_id = dm.merchantid
    LEFT JOIN links AS l ON p.product_id = l.product_id
    LEFT JOIN docs AS d
        ON
            l.doc_id = d.doc_id
            AND dm.doc_id = d.doc_id

    LEFT JOIN links_v2 AS lv ON p.product_id = lv.product_id
    LEFT JOIN docs AS dv ON lv.doc_id = dv.doc_id

    LEFT JOIN {{ ref('gold_merchant_categories') }} AS gc ON p.merchant_category_id = gc.merchant_category_id
    LEFT JOIN categories_ege AS c ON p.merchant_category_id = c.merchant_category_id
    LEFT JOIN categories_ce_fda_pi_doc_full AS cc ON p.merchant_category_id = cc.merchant_category_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    HAVING
        MAX(COALESCE(d.type = "ec" AND d.status = "active" AND cc.merchant_category_id IS NOT NULL, FALSE)) IS FALSE
        OR MAX(COALESCE(d.type = "doc" AND d.status = "active" AND cc.merchant_category_id IS NOT NULL, FALSE)) IS FALSE
        OR MAX(COALESCE(d.type = "eprGermanyElectronics" AND d.status = "active" AND c.merchant_category_id IS NOT NULL, FALSE)) IS FALSE
        OR MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprGermany" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprGermany" AND d.status = "active", FALSE)
            END
        ) IS FALSE
        OR MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprFrance" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = TRUE THEN COALESCE(d.type = "eprFrance" AND d.status = "active", FALSE)
            END
        ) IS FALSE
        OR MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprAustria" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprAustria" AND d.status = "active", FALSE)
            END
        ) IS FALSE
        OR MAX(COALESCE(dv.type = "ec" AND dv.status = "active", FALSE)) IS FALSE
        OR MAX(COALESCE(dv.type = "doc" AND dv.status = "active", FALSE)) IS FALSE
        OR MAX(COALESCE(dv.type = "fda" AND dv.status = "active", FALSE)) IS FALSE

        OR MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprSweden" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprSweden" AND d.status = "active", FALSE)
            END
        ) IS FALSE

        OR MAX(
            CASE
                WHEN dm.is_packaging = TRUE THEN COALESCE(dm.type = "eprSpain" AND dm.status = "active", FALSE)
                WHEN dm.is_packaging = FALSE THEN COALESCE(d.type = "eprSpain" AND d.status = "active", FALSE)
            END
        ) IS FALSE

        OR MAX(COALESCE(dm.type = "ear" AND dm.status = "active", FALSE)) IS FALSE
)

SELECT
    a.*,
    COALESCE(ROUND(g.gmv_30_day_all, 2), 0) AS gmv_30_day_all,
    COALESCE(ROUND(g.gmv_90_day_all, 2), 0) AS gmv_90_day_all,
    COALESCE(ROUND(g.gmv_180_day_all, 2), 0) AS gmv_180_day_all,
    COALESCE(ROUND(g.gmv_30_day_fr, 2), 0) AS gmv_30_day_fr,
    COALESCE(ROUND(g.gmv_90_day_fr, 2), 0) AS gmv_90_day_fr,
    COALESCE(ROUND(g.gmv_180_day_fr, 2), 0) AS gmv_180_day_fr,
    COALESCE(ROUND(g.gmv_30_day_de, 2), 0) AS gmv_30_day_de,
    COALESCE(ROUND(g.gmv_90_day_de, 2), 0) AS gmv_90_day_de,
    COALESCE(ROUND(g.gmv_180_day_de, 2), 0) AS gmv_180_day_de,

    COALESCE(ROUND(g.gmv_30_day_at, 2), 0) AS gmv_30_day_at,
    COALESCE(ROUND(g.gmv_90_day_at, 2), 0) AS gmv_90_day_at,
    COALESCE(ROUND(g.gmv_180_day_at, 2), 0) AS gmv_180_day_at,

    COALESCE(ROUND(g.gmv_30_day_eu, 2), 0) AS gmv_30_day_eu,
    COALESCE(ROUND(g.gmv_90_day_eu, 2), 0) AS gmv_90_day_eu,
    COALESCE(ROUND(g.gmv_180_day_eu, 2), 0) AS gmv_180_day_eu,

    COALESCE(ROUND(g.gmv_30_day_fi, 2), 0) AS gmv_30_day_fi,
    COALESCE(ROUND(g.gmv_90_day_fi, 2), 0) AS gmv_90_day_fi,
    COALESCE(ROUND(g.gmv_180_day_fi, 2), 0) AS gmv_180_day_fi,

    COALESCE(ROUND(g.gmv_30_day_se, 2), 0) AS gmv_30_day_se,
    COALESCE(ROUND(g.gmv_90_day_se, 2), 0) AS gmv_90_day_se,
    COALESCE(ROUND(g.gmv_180_day_se, 2), 0) AS gmv_180_day_se,

    COALESCE(ROUND(g.gmv_30_day_es, 2), 0) AS gmv_30_day_es,
    COALESCE(ROUND(g.gmv_90_day_es, 2), 0) AS gmv_90_day_es,
    COALESCE(ROUND(g.gmv_180_day_es, 2), 0) AS gmv_180_day_es
FROM agg AS a
LEFT JOIN gmv AS g ON a.product_id = g.product_id
