{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

---------------------------------------------------------------------

-- RU: Собираем pzn, у которых есть аналоги, из других medical_id групп 
-- ENG: Collect pzn that have analogs from other medical_id groups 

-- They are the same analogues
--     the same active ingredients
--     the same extra ingredients
--     the same form
--     the same manufacture

---------------------------------------------------------------------


WITH product_ingredient AS (
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
    ORDER BY
        pzn,
        dosage_form_short_name,
        ingredient.name,
        medicine_ingredient.index,
        medicine_ingredient.quantity,
        medicine_ingredient.unit
),

base AS (
    SELECT
        pzn,
        product_id,
        medicine_group_id,
        manufacturer_id,
        manufacturer_name,
        dosage_form_short_name,
        dosage_form_long_name
    FROM product_ingredient
    GROUP BY
        pzn,
        product_id,
        medicine_group_id,
        manufacturer_id,
        manufacturer_name,
        dosage_form_short_name,
        dosage_form_long_name
    ORDER BY
        pzn,
        product_id,
        medicine_group_id,
        manufacturer_id,
        manufacturer_name,
        dosage_form_short_name,
        dosage_form_long_name
),

active_ingredients AS (
    SELECT
        product_id,
        SORT_ARRAY(COLLECT_LIST(STRUCT(index, name, quantity, unit))) AS tmp_active
    FROM product_ingredient
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
    FROM product_ingredient
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
        actives.active,
        extras.extra,
        STRUCT(actives.active AS active, extras.extra AS extra) AS ingredients
    FROM base
    LEFT JOIN actives
        ON base.product_id = actives.product_id
    LEFT JOIN extras
        ON base.product_id = extras.product_id
)

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
    ARRAY_AGG(analogs.pzn) AS analog_pzns,
    COUNT(analogs.pzn) AS analogs_count
FROM final AS base
LEFT JOIN final AS analogs
    ON
        SHA2(TO_JSON(base.ingredients), 256) = SHA2(TO_JSON(analogs.ingredients), 256)
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
    base.ingredients
HAVING
    analogs_count > 0