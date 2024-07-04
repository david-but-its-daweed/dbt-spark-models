{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true',
    }
) }}


SELECT     _id as product_id,
           categoryId as category_id,
           category_name,
           millis_to_ts_msk(createdTimeMs) as created_ts_msk,
           date(millis_to_ts_msk(createdTimeMs)) as created_date,
           dangerousKind as dangerous_kind,
           merchantId as merchant_id,
           origDescription as orig_description,
           origExtraImageUrls as orig_extra_image_urls,
           origMainImageUrl as orig_main_image_url,
           origName as orig_name,
           origUrl as orig_url,
           sku,
           storeId as store_id,
           millis_to_ts_msk(updatedTimeMs) as update_ts_msk,
           date(millis_to_ts_msk(updatedTimeMs)) as update_date,
           millis_to_ts_msk(publishedTimeMs) as published_ts_msk,
           date(millis_to_ts_msk(publishedTimeMs)) as published_date
from {{ ref('scd2_published_products_snapshot')}} pp
left join 
(
select category_id, name as category_name
    from {{ source('mart', 'category_levels') }}
) cat on pp.categoryId = cat.category_id
where dbt_valid_to is null
and merchantId = '66054380c33acc34a54a56d0'