{{
  config(
    meta = {
        'model_owner' : '@catman-analytics.duty'
    }
  )
}}

SELECT
    main.partition_date,
    main.product_variant_id,
    main.product_id,
    gp.business_line,
    main.logistics_product_id,
    main.number_of_reserved,
    main.number_of_products_in_stock,
    main.stock_update_ts,
    main.brand_name,
    main.product_name,
    main.product_url,
    main.product_dimensions,
    main.merchant_id,
    main.original_merchant_price,
    main.product_weight
FROM models.fbj_product_stocks AS main
LEFT JOIN gold.products AS gp ON gp.product_id = main.product_id
