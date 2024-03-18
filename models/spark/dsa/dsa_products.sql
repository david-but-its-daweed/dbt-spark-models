{{
  config(
    meta = {
      'model_owner' : '@gusev',
    },
    materialized='table'
  )
}}

SELECT DISTINCT product_id
FROM {{ source('mart', 'published_products_current') }}
WHERE labels[0]['key'] = 'dangerousProductRecall'