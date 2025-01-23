{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}


WITH product_ingredient AS (
    SELECT
        medicine.country_local_id AS pzn,
        ingredient.name AS name,
        medicine_ingredient.quantity AS quantity,
        medicine_ingredient.unit AS unit,
        medicine_ingredient.active AS is_active,
        dosage_form.short_name AS dosage_form_short_name,
        dosage_form.long_name AS dosage_form_long_name
    FROM {{ source('pharmacy_landing', 'medicine_ingredient') }} AS medicine_ingredient
    INNER JOIN {{ source('pharmacy_landing', 'ingredient') }} AS ingredient
        ON medicine_ingredient.ingredient_id = ingredient.id
    INNER JOIN {{ source('pharmacy_landing', 'medicine') }} AS medicine
        ON medicine_ingredient.medicine_id = medicine.id
    INNER JOIN {{ source('pharmacy_landing', 'dosage_form') }} AS dosage_form
        ON medicine.dosage_form_id = dosage_form.id
    WHERE
        1 = 1
        AND medicine_ingredient.active = TRUE
        AND medicine_ingredient.quantity IS NOT NULL
        AND medicine_ingredient.unit IS NOT NULL
    ORDER BY
        pzn,
        dosage_form_short_name,
        name,
        quantity,
        unit
),

actives AS (
    SELECT
        pzn,
        name,
        quantity,
        unit,
        is_active,
        dosage_form_short_name,
        dosage_form_long_name,
        ARRAY_JOIN(ARRAY_AGG(CONCAT(name, ' ', quantity, ' ', unit, ' ', dosage_form_short_name)), ', ') AS actives
    FROM product_ingredient
    GROUP BY
        pzn,
        name,
        quantity,
        unit,
        is_active,
        dosage_form_short_name,
        dosage_form_long_name
)

SELECT
    actives.pzn AS pzn,
    actives.name,
    actives.quantity,
    actives.unit,
    actives.is_active,
    actives.dosage_form_short_name,
    actives.dosage_form_long_name,
    actives.actives,
    ARRAY_AGG(analogs.pzn) AS analog_pzns,
    COUNT(analogs.pzn) AS analogs_count
FROM actives
LEFT JOIN actives AS analogs
    ON
        actives.actives = analogs.actives
        AND actives.pzn != analogs.pzn
GROUP BY
    actives.pzn,
    actives.name,
    actives.quantity,
    actives.unit,
    actives.is_active,
    actives.dosage_form_short_name,
    actives.dosage_form_long_name,
    actives.actives
