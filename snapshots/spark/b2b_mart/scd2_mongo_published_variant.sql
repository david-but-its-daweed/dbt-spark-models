{% snapshot scd2_mongo_published_variant %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='variant_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

SELECT   _id AS variant_id,
         millis_to_ts_msk(createdTimeMs) AS created_ts_msk,
         enabled,
         gtin,
         hsCode AS hs_code,
         merchantId AS merchant_id,
         origColors AS orig_colors,
         origMainImageUrl AS orig_main_image_url,
         productid AS product_id,
         public,
         millis_to_ts_msk(publishedTimeMs) AS published_ts_msk,
         shippingWeight AS shipping_weight,
         sku,
         millis_to_ts_msk(updatedTimeMs) AS update_ts_msk
FROM {{ source('mongo', 'b2b_core_published_variants_daily_snapshot') }}
{% endsnapshot %}
