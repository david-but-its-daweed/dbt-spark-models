{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    partition_by=['order_day'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'order_day',
      'bigquery_partition_date': '{{ next_ds }}',
      'bigquery_upload_horizon_days': '30',
      'bigquery_fail_on_missing_partitions': 'false',
    }
) }}

--------------------------------------------------------------
-- определяем категорийное дерево для продуктов
-- если у продукта 2 или более катеогорий, остаются все категории
--------------------------------------------------------------
WITH product_categories AS (
    SELECT
        product_id,
        category_id AS product_category,
        category_1.slug AS product_category_slug,
        CASE
            WHEN category_1.parent_category_id IS NULL AND category_1.id IS NOT NULL THEN category_1.id
            WHEN category_2.parent_category_id IS NULL AND category_2.id IS NOT NULL THEN category_2.id
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_3.id
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_4.id
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_5.id
        END AS category_id_level_1,
        CASE
            WHEN category_2.parent_category_id IS NULL AND category_2.id IS NOT NULL THEN category_1.id
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_2.id
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_3.id
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_4.id
        END AS category_id_level_2,
        CASE
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_1.id
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_2.id
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_3.id
        END AS category_id_level_3,
        CASE
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_1.id
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_2.id
        END AS category_id_level_4,
        CASE
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_1.id
        END AS category_id_level_5,

        CASE
            WHEN category_1.parent_category_id IS NULL AND category_1.id IS NOT NULL THEN category_1.name
            WHEN category_2.parent_category_id IS NULL AND category_2.id IS NOT NULL THEN category_2.name
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_3.name
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_4.name
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_5.name
        END AS category_name_level_1,
        CASE
            WHEN category_2.parent_category_id IS NULL AND category_2.id IS NOT NULL THEN category_1.name
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_2.name
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_3.name
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_4.name
        END AS category_name_level_2,
        CASE
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_1.name
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_2.name
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_3.name
        END AS category_name_level_3,
        CASE
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_1.name
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_2.name
        END AS category_name_level_4,
        CASE
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_1.name
        END AS category_name_level_5,

        CASE
            WHEN category_1.parent_category_id IS NULL AND category_1.id IS NOT NULL THEN category_1.slug
            WHEN category_2.parent_category_id IS NULL AND category_2.id IS NOT NULL THEN category_2.slug
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_3.slug
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_4.slug
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_5.slug
        END AS category_slug_level_1,
        CASE
            WHEN category_2.parent_category_id IS NULL AND category_2.id IS NOT NULL THEN category_1.slug
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_2.slug
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_3.slug
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_4.slug
        END AS category_slug_level_2,
        CASE
            WHEN category_3.parent_category_id IS NULL AND category_3.id IS NOT NULL THEN category_1.slug
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_2.slug
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_3.slug
        END AS category_slug_level_3,
        CASE
            WHEN category_4.parent_category_id IS NULL AND category_4.id IS NOT NULL THEN category_1.slug
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_2.slug
        END AS category_slug_level_4,
        CASE
            WHEN category_5.parent_category_id IS NULL AND category_5.id IS NOT NULL THEN category_1.slug
        END AS category_slug_level_5

    FROM {{ source('pharmacy_landing', 'product_category_flat') }} AS product_category_flat
    LEFT JOIN {{ source('pharmacy_landing', 'category') }} AS category_1
        ON product_category_flat.category_id = category_1.id
    LEFT JOIN {{ source('pharmacy_landing', 'category') }} AS category_2
        ON category_1.parent_category_id = category_2.id
    LEFT JOIN {{ source('pharmacy_landing', 'category') }} AS category_3
        ON category_2.parent_category_id = category_3.id
    LEFT JOIN {{ source('pharmacy_landing', 'category') }} AS category_4
        ON category_3.parent_category_id = category_4.id
    LEFT JOIN {{ source('pharmacy_landing', 'category') }} AS category_5
        ON category_4.parent_category_id = category_5.id
),

--------------------------------------------------------------
-- забираем только те продукты, которые были проданы
--------------------------------------------------------------
sold_products AS (
    SELECT
        CAST(DATE_TRUNC('day', order_created_time_cet) AS DATE) AS order_day,
        store_name,
        pzn,
        product_id,
        product_name,
        SUM(before_products_price) AS before_products_price,
        SUM(products_price) AS products_price,
        SUM(quantity) AS quantity,
        COUNT(DISTINCT order_id) AS orders
    FROM {{ source('onfy', 'orders_info') }}
    GROUP BY
        DATE_TRUNC('day', order_created_time_cet),
        store_name,
        pzn,
        product_id,
        product_name
),

--------------------------------------------------------------
-- добираем информацию о продукте
--------------------------------------------------------------
products_info AS (
    SELECT DISTINCT
        product.id AS product_id,
        manufacturer_id,
        manufacturer.name AS manufacturer_name,
        medicine.quantity AS medicine_quantity,
        medicine.unit AS medicine_unit,
        product.slug AS product_slug,
        onfy_medicine_analogs.actives AS actives_list,
        onfy_medicine_analogs.analog_pzns,
        COALESCE(onfy_medicine_analogs.analogs_count, 0) AS analogs_count
    FROM {{ source('pharmacy_landing', 'product') }} AS product
    LEFT JOIN {{ source('pharmacy_landing', 'manufacturer') }} AS manufacturer
        ON product.manufacturer_id = manufacturer.id
    LEFT JOIN {{ source('pharmacy_landing', 'medicine') }} AS medicine
        ON product.id = medicine.id
    LEFT JOIN {{ source('onfy', 'onfy_medicine_analogs') }} AS onfy_medicine_analogs
        ON medicine.country_local_id = onfy_medicine_analogs.pzn
)

SELECT
    sold_products.*,
    manufacturer_id,
    manufacturer_name,
    medicine_quantity,
    medicine_unit,
    product_slug,
    product_category,

    category_id_level_1,
    category_id_level_2,
    category_id_level_3,
    category_id_level_4,
    category_id_level_5,

    COALESCE(category_name_level_1, 'NO CATEGORY') AS category_name_level_1,
    category_name_level_2,
    category_name_level_3,
    category_name_level_4,
    category_name_level_5,

    COALESCE(category_slug_level_1, 'NO CATEGORY') AS category_slug_level_1,
    category_slug_level_2,
    category_slug_level_3,
    category_slug_level_4,
    category_slug_level_5,

    CONCAT('https://onfy.de/katalog/', product_category, '/', product_category_slug) AS catalog_url,
    CONCAT('https://onfy.de/artikel/', pzn, '/', product_slug) AS product_url,

    COUNT(product_category) OVER (PARTITION BY order_day, pzn, manufacturer_id) AS categories_number,

    actives_list,
    analog_pzns,
    analogs_count
FROM sold_products
INNER JOIN products_info
    USING (product_id)
LEFT JOIN product_categories
    USING (product_id)
DISTRIBUTE BY order_day
