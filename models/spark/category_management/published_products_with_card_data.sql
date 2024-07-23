{{ config(
    schema='category_management',
    materialized='table',
    meta = {
      'model_owner' : '@catman-analytics.duty',
      'team': 'category_management',
      'bigquery_load': 'true',
    }
) }}

select
    product_id,
    videoId as video_id,
    brandId as brand_id,
    class as brand_class,
    brandAuth as brand_auth,
    1 + size(extraImages) as images_count,
    size (origTags) as original_tags_count,
    concat_ws(',', origTags) as original_tags

from 
    {{ source('mart', 'published_products_current') }}
left join {{ source('mongo', 'product_products_daily_snapshot') }}
    on published_products_current.product_id = product_products_daily_snapshot._id
left join  {{ source('mongo','abu_core_brands_daily_snapshot') }} as brands
    on product_products_daily_snapshot.brandId = brands._id
