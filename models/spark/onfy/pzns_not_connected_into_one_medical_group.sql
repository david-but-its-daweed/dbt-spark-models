{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

---------------------------------------------------------------------

-- RU: Собираем pzn, у которых есть аналоги, из других medical_id групп (при условии наличия хотя бы одного заказа в истории)
-- ENG: Collect pzn that have analogs from other medical_id groups (if there has been at least one order in history)

-- They are the same analogues
--     the same active ingredients
--     the same extra ingredients
--     the same form
--     the same manufacture

---------------------------------------------------------------------

WITH pzns_with_orders AS (
    SELECT
        pzn,
        SUM(products_price) AS gmv
    FROM {{ ref('orders_info') }}
    GROUP BY pzn
),

product_ingredient AS (
    SELECT
        medicine.country_local_id AS pzn,
        medicine_ingredient.medicine_id AS product_id,
        medicine.medicine_group_id,
        product.manufacturer_id,
        product.name AS manufacturer_name,
        ingredient.name,
        medicine_ingredient.index,
        medicine_ingredient.quantity,
        medicine_ingredient.unit,
        medicine_ingredient.active AS is_active,
        dosage_form.short_name AS dosage_form_short_name,
        dosage_form.long_name AS dosage_form_long_name
    FROM {{ source('pharmacy_landing', 'medicine_ingredient') }} AS medicine_ingredient
    LEFT JOIN {{ source('pharmacy_landing', 'ingredient') }} AS ingredient
        ON medicine_ingredient.ingredient_id = ingredient.id
    LEFT JOIN {{ source('pharmacy_landing', 'medicine') }} AS medicine
        ON medicine.id = medicine_ingredient.medicine_id
    LEFT JOIN {{ source('pharmacy_landing', 'dosage_form') }} AS dosage_form
        ON dosage_form.id = medicine.dosage_form_id
    LEFT JOIN {{ source('pharmacy_landing', 'product') }} AS product
        ON product.id = medicine.id
    WHERE
        medicine.country_local_id IS NOT NULL
        AND ingredient.name IS NOT NULL
        AND medicine_ingredient.quantity IS NOT NULL
        AND medicine_ingredient.unit IS NOT NULL
        AND dosage_form.short_name IS NOT NULL
),

product_ingredient_with_orders AS (
    SELECT
        product_ingredient.*,
        pzns_with_orders.gmv
    FROM product_ingredient
    INNER JOIN
        pzns_with_orders
        ON product_ingredient.pzn = pzns_with_orders.pzn
),

base AS (
    SELECT
        pzn,
        product_id,
        medicine_group_id,
        manufacturer_id,
        manufacturer_name,
        dosage_form_short_name,
        dosage_form_long_name,
        gmv
    FROM product_ingredient_with_orders
    GROUP BY
        pzn,
        product_id,
        medicine_group_id,
        manufacturer_id,
        manufacturer_name,
        dosage_form_short_name,
        dosage_form_long_name,
        gmv
    ORDER BY
        pzn,
        product_id,
        medicine_group_id,
        manufacturer_id,
        manufacturer_name,
        dosage_form_short_name,
        dosage_form_long_name,
        gmv
),

active_ingredients AS (
    SELECT
        product_id,
        SORT_ARRAY(COLLECT_LIST(STRUCT(index, name, quantity, unit))) AS tmp_active
    FROM product_ingredient_with_orders
    WHERE is_active = TRUE
    GROUP BY product_id
),

actives AS (
    SELECT
        product_id,
        TRANSFORM(tmp_active, x -> STRUCT(x.name AS name, x.quantity AS quantity, x.unit AS unit)) AS active
    FROM active_ingredients
),

extra_ingredients AS (
    SELECT
        product_id,
        SORT_ARRAY(COLLECT_LIST(STRUCT(index, name, quantity, unit))) AS tmp_extra
    FROM product_ingredient_with_orders
    WHERE is_active = FALSE
    GROUP BY product_id
),

extras AS (
    SELECT
        product_id,
        TRANSFORM(tmp_extra, x -> STRUCT(x.name AS name, x.quantity AS quantity, x.unit AS unit)) AS extra
    FROM extra_ingredients
),

final AS (
    SELECT
        base.pzn,
        base.product_id,
        base.medicine_group_id,
        base.manufacturer_id,
        base.manufacturer_name,
        base.dosage_form_short_name,
        base.dosage_form_long_name,
        base.gmv,
        actives.active,
        extras.extra,
        STRUCT(actives.active AS active, extras.extra AS extra) AS ingredients
    FROM base
    LEFT JOIN actives
        ON base.product_id = actives.product_id
    LEFT JOIN extras
        ON base.product_id = extras.product_id
),

full_joined_result AS (
    SELECT
        base.pzn,
        base.product_id,
        base.medicine_group_id,
        base.manufacturer_id,
        base.manufacturer_name,
        base.dosage_form_short_name,
        base.dosage_form_long_name,
        base.active,
        base.extra,
        base.ingredients,
        base.gmv,
        SORT_ARRAY(ARRAY_AGG(DISTINCT analogs_all.pzn)) AS analog_pzns_all,
        SORT_ARRAY(ARRAY_AGG(DISTINCT analogs_all.medicine_group_id)) AS analog_medicine_group_ids_all,
        COUNT(DISTINCT analogs_all.pzn) AS analogs_count_all,
        SORT_ARRAY(ARRAY_AGG(DISTINCT analogs.pzn)) AS analog_pzns,
        SORT_ARRAY(ARRAY_AGG(DISTINCT analogs.medicine_group_id)) AS analog_medicine_group_ids,
        COUNT(DISTINCT analogs.pzn) AS analogs_count
    FROM final AS base
    LEFT JOIN final AS analogs_all -- To gather analogs from the same medical_id_group (including target pzn) for ranking
        ON
            base.ingredients = analogs_all.ingredients
            AND base.dosage_form_short_name = analogs_all.dosage_form_short_name
            AND base.manufacturer_id = analogs_all.manufacturer_id
    LEFT JOIN final AS analogs -- To gather analogs from different medical_id_groups
        ON
            base.ingredients = analogs.ingredients
            AND base.product_id != analogs.product_id
            AND base.medicine_group_id != analogs.medicine_group_id
            AND base.dosage_form_short_name = analogs.dosage_form_short_name
            AND base.manufacturer_id = analogs.manufacturer_id
    GROUP BY
        base.pzn,
        base.product_id,
        base.medicine_group_id,
        base.manufacturer_id,
        base.manufacturer_name,
        base.dosage_form_short_name,
        base.dosage_form_long_name,
        base.active,
        base.extra,
        base.ingredients,
        base.gmv
    HAVING
        analogs_count > 0
),

distinctArrays AS (
    SELECT DISTINCT analog_pzns_all AS sortedArray
    FROM full_joined_result
),

rankedArrays AS (
    SELECT
        sortedArray,
        DENSE_RANK() OVER (ORDER BY CONCAT_WS(',', sortedArray)) AS group_id
    FROM distinctArrays
)

SELECT
    r.group_id AS new_cluster_id,
    full_joined_result.medicine_group_id,
    full_joined_result.pzn,
    full_joined_result.product_id,
    full_joined_result.manufacturer_name AS product_name,
    full_joined_result.manufacturer_id,
    full_joined_result.dosage_form_short_name,
    full_joined_result.gmv,
    SUM(full_joined_result.gmv) OVER (PARTITION BY r.group_id) AS cluster_gmv
FROM full_joined_result
INNER JOIN rankedArrays AS r
    ON full_joined_result.analog_pzns_all = r.sortedArray
ORDER BY cluster_gmv DESC