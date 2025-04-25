{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'priority_weight': '150',
      'model_owner' : '@vasiukova_mn',
      'bigquery_load': 'true'
    }
) }}

SELECT
    product_id,
    LAST_VALUE(product_outlier_group_name) AS product_outlier_group_name
FROM {{ ref("product_outliers_seed") }}
GROUP BY 1
