{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    alias='js2_product_clusters',
    file_format='delta',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    meta = {
        'model_owner' : '@vasiukova_mn',
        'bigquery_load': 'true'
    },
  )
}}

WITH cluster_links_raw AS (
    SELECT
        _id AS cluster_link_id,
        cid AS cluster_id,
        pId AS product_id,
        EXPLODE(vLinks) AS vl,
        clt,
        ARRAY_SORT(sh, (left, right) -> CASE WHEN left.ut > right.ut THEN 1 ELSE 0 END)[0].cs AS cls
    FROM {{ source('mongo', 'product_product_cluster_links_daily_snapshot') }}
),

cluster_links AS (
    SELECT
        *,
        vl.vid AS variant_id,
        vl.cvid AS cluster_variant_id,
        CASE
            WHEN clt = 1 THEN "ByMerchant"
            WHEN clt = 2 THEN "AutoLink"
            WHEN clt = 3 THEN "Moderator"
            ELSE "Unknown"
        END AS store_link_type,
        CASE
            WHEN cls = 0 THEN "Pending"
            WHEN cls = 1 THEN "Approved"
            WHEN cls = 2 THEN "Rejected"
            WHEN cls = 3 THEN "Archived"
            WHEN cls = 4 THEN "Draft"
            ELSE "Unknown"
        END AS current_link_status
    FROM cluster_links_raw
),

clusters AS (
    SELECT
        _id AS cluster_id,
        initProdId AS initial_product_id,
        n AS cluster_name,
        ct AS cluster_created_at,
        ver AS version,
        CASE
            WHEN st = 0 THEN "Enabled"
            WHEN st = 1 THEN "Invisible"
            WHEN st = 2 THEN "Disabled"
            ELSE "Unknown"
        END AS cluster_state,
        EXPLODE(vars) AS vars
    FROM {{ source('mongo', 'product_product_clusters_daily_snapshot') }}
)

SELECT
    c.cluster_id,
    c.initial_product_id,
    c.cluster_name,
    c.cluster_created_at,
    c.version,
    c.cluster_state,
    l.cluster_link_id,
    l.product_id,
    l.variant_id,
    l.cluster_variant_id,
    l.store_link_type,
    l.current_link_status
FROM clusters AS c
INNER JOIN cluster_links AS l ON
    l.cluster_id = c.cluster_id
    AND l.cluster_variant_id = c.vars._id