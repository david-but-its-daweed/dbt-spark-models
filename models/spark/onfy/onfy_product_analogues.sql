{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@andrewocean',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_overwrite': 'true'
    }
) }}


WITH ingredient_data AS (
    SELECT
        mi.medicine_id AS product_id,
        m.country_local_id AS pzn,
        mi.ingredient_id,
        i.name,
        mi.active,
        mi.index,
        mi.quantity,
        mi.unit,
        df.short_name AS dosage_form
    FROM {{ source('pharmacy_landing', 'medicine_ingredient') }} AS mi
    INNER JOIN {{ source('pharmacy_landing', 'ingredient') }} AS i
        ON mi.ingredient_id = i.id
    INNER JOIN {{ source('pharmacy_landing', 'medicine') }} AS m
        ON mi.medicine_id = m.id
    INNER JOIN {{ source('pharmacy_landing', 'dosage_form') }} AS df
        ON m.dosage_form_id = df.id
),

active AS (
    SELECT
        product_id,
        TRANSFORM(
            SORT_ARRAY(COLLECT_LIST(STRUCT(index, name, quantity, unit, dosage_form))),
            x -> STRUCT(x.name AS name, x.quantity AS quantity, x.unit AS unit, x.dosage_form AS dosage_form)
        ) AS active
    FROM ingredient_data
    WHERE active = TRUE
    GROUP BY product_id
),

with_signature AS (
    SELECT
        product_id,
        active,
        IF(
            EXISTS(active, x -> x.quantity IS NULL),
            NULL,
            SHA2(CONCAT_WS('|', TRANSFORM(SORT_ARRAY(active), x -> CONCAT_WS(':', x.name, CAST(x.quantity AS STRING), x.unit, x.dosage_form))), 256)
        ) AS full_signature
    FROM active
),

grouped AS (
    SELECT
        full_signature,
        COLLECT_LIST(product_id) AS product_ids
    FROM with_signature
    WHERE full_signature IS NOT NULL
    GROUP BY full_signature
    HAVING SIZE(product_ids) > 1
),

-- unique pairs generating (product_id, analogue_product_id)
pairs AS (
    SELECT
        g.product_ids[i] AS product_id,
        g.product_ids[j] AS analogue_product_id
    FROM grouped AS g
        LATERAL VIEW POSEXPLODE(g.product_ids) i_view AS i, p1
        LATERAL VIEW POSEXPLODE(g.product_ids) j_view AS j, p2
    WHERE j > i  -- only unique pairs without selfs
),

product_names AS (
    SELECT
        product_id,
        pzn,
        product_name,
        CONCAT(quantity, ' ', unit) AS package_size,
        manufacturer_short_name AS manufacturer,
        MIN(price) AS min_price
    FROM {{ source('onfy_mart', 'dim_product') }}
    WHERE
        TRUE
        AND is_current
        AND NOT is_deleted
        AND store_state != 'RESTRICTED' -- only active pharmacies
        AND legal_form != 'RX' -- not prescribed products (without recipe)
        AND stock_quantity > 0 -- available for purchase at the moment
    GROUP BY 1, 2, 3, 4, 5
)


SELECT
    pn.product_id,
    pn.pzn AS product_pzn,
    pn.product_name,
    pn.package_size AS product_package_size,
    pn.manufacturer AS product_manufacturer,
    pn.min_price AS product_min_price,
    p.analogue_product_id AS analogue_id,
    pna.pzn AS analogue_pzn,
    pna.product_name AS analogue_name,
    pna.package_size AS analogue_package_size,
    pna.manufacturer AS analogue_manufacturer,
    pna.min_price AS analogue_min_price
FROM product_names AS pn
INNER JOIN pairs AS p
    ON pn.product_id = p.product_id
INNER JOIN product_names AS pna
    ON p.analogue_product_id = pna.product_id
WHERE
    TRUE
    AND p.analogue_product_id IS NOT NULL
    AND pn.manufacturer != pna.manufacturer
