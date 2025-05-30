{% snapshot scd2_mongo_published_product %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='product_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

SELECT  _id as product_id,
           categoryId as category_id,
           millis_to_ts_msk(createdTimeMs) as created_ts_msk,
           dangerousKind as dangerous_kind,
           merchantId as merchant_id,
           origDescription as orig_description,
           origExtraImageUrls as orig_extra_image_urls,
           origMainImageUrl as orig_main_image_url,
           origName as orig_name,
           origUrl as orig_url,
           sku,
           storeId as store_id,
           millis_to_ts_msk(updatedTimeMs) as update_ts_msk
FROM {{ source('mongo', 'b2b_product_published_products_daily_snapshot') }}
{% endsnapshot %}
