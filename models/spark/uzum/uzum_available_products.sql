{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    alias='uzum_available_products',
    file_format='delta',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    meta = {
        'model_owner' : '@vasiukova_mn'
    },
  )
}}

WITH stopwords_pattern AS (
    SELECT CONCAT('.*', ARRAY_JOIN(COLLECT_LIST(LOWER(stopword)), '|'), '.*') AS pattern
    FROM {{ ref('uzum_stopwords') }}
),

refurbished_merchants AS (
    SELECT e.id AS merchant_id
    FROM {{ source('mongo', 'core_entity_labels_entries_daily_snapshot') }}
    WHERE k = 'refurbishedWhiteList'
)

SELECT p._id AS product_id
FROM {{ source('mongo', 'product_products_daily_snapshot') }} AS p
INNER JOIN {{ ref('gold_merchants') }} AS m ON p.merchantId = m.merchant_id
INNER JOIN {{ source('mart', 'published_products_current') }} AS c ON p._id = c.product_id
INNER JOIN {{ ref('gold_merchant_categories') }} AS mc ON c.category_id = mc.merchant_category_id
LEFT JOIN refurbished_merchants AS r ON p.merchantId = r.merchant_id
WHERE
    p.public AND p.hasActive AND p.enabledByMerchant AND m.origin_name = 'Chinese'
    AND (c.rating IS null OR c.rating > 3.4)
    AND NOT mc.l1_merchant_category_id IN (SELECT t.merchant_category_id FROM {{ ref('uzum_restricted_categories') }} AS t)
    AND NOT mc.l2_merchant_category_id IN (SELECT t.merchant_category_id FROM {{ ref('uzum_restricted_categories') }} AS t)
    AND NOT mc.l3_merchant_category_id IN (SELECT t.merchant_category_id FROM {{ ref('uzum_restricted_categories') }} AS t)
    AND NOT COALESCE(mc.l4_merchant_category_id, '') IN (SELECT t.merchant_category_id FROM {{ ref('uzum_restricted_categories') }} AS t)
    AND NOT COALESCE(mc.l5_merchant_category_id, '') IN (SELECT t.merchant_category_id FROM {{ ref('uzum_restricted_categories') }} AS t)
    AND (NOT ARRAYS_OVERLAP(c.labels.key, ARRAY('LGBTSymbols', 'refurbishedOfficially', 'labubu')) OR ARRAY_SIZE(c.labels) IS null)
    AND r.merchant_id IS null
    AND NOT REGEXP_LIKE(LOWER(p.origName), (SELECT s.pattern FROM stopwords_pattern AS s))
    AND NOT REGEXP_LIKE(LOWER(p.origDescription), (SELECT s.pattern FROM stopwords_pattern AS s))