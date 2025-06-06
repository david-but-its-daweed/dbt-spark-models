{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@general_analytics',
    }
  )
}}

SELECT DISTINCT
    product_id,
    labels[0]['key'] AS reason
FROM {{ source('mart', 'published_products_current') }}
WHERE labels[0]['key'] = 'dangerousProductRecall'
