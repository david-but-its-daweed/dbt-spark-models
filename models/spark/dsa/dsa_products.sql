{{
  config(
    meta = {
      'model_owner' : '@gusev',
    },
    materialized='table'
  )
}}

SELECT DISTINCT
    product_id,
    labels[0]['key'] = 'dangerousProductRecall' AS reason
FROM {{ source('mart', 'published_products_current') }}
WHERE labels[0]['key'] = 'dangerousProductRecall'