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

    SELECT
        _id AS variant_id,
        MILLIS_TO_TS_MSK(createdTimeMs) AS created_ts_msk,
        enabled,
        gtin,
        hsCode AS hs_code,
        merchantId AS merchant_id,
        origColors AS orig_colors,
        origMainImageUrl AS orig_main_image_url,
        productid AS product_id,
        MILLIS_TO_TS_MSK(createdTimeMs) AS published_ts_msk,
        shippingWeight AS shipping_weight,
        sku,
        MILLIS_TO_TS_MSK(updatedTimeMs) AS update_ts_msk
    FROM {{ source('mongo', 'b2b_core_published_variants_daily_snapshot') }}
{% endsnapshot %}
