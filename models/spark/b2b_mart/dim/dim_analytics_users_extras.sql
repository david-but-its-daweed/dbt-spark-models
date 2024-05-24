{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH t1 AS (
SELECT
    _id,
    explode(transform(categories, c -> replace(c, '1,', ''))) id
FROM {{ source('mongo', 'b2b_core_analytics_users_extras_daily_snapshot') }}
), c AS (
    SELECT
        _id,
        concat_ws(',', collect_list(c.name)) categories
    FROM t1
    JOIN {{ source('joompro_analytics', 'mercadolibre_category_info') }} c USING (id)
    group by 1
)
SELECT
    _id AS user_id,
    coalesce(mlhs, hasMercadolivreStore) AS has_ml_store,
    mercadolivreShopName AS ml_shop_name,
    coalesce(hasOtherStores, false) AS sells_on_other_marketplaces,
    concat_ws(',', otherMarketName) AS marketplaces,
    hasImport AS does_import,
    c.categories,
    mlsl AS store_link,
    concat(concat_ws(',', whatToDoChecked), ',', whatToDoOther) AS todos,
    coalesce(forceNonPremium, false) AS force_non_premium,
    coalesce(forcePremium, false) AS force_premium
FROM {{ source('mongo', 'b2b_core_analytics_users_extras_daily_snapshot') }}
JOIN c USING (_id)
