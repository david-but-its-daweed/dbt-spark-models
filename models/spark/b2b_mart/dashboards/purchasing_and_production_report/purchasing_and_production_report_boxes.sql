{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH request AS (
    SELECT
        customer_request_id,
        SUM(qty) AS qty,
        ROUND(SUM(merchant_price_per_item * qty) / SUM(qty), 2) AS merchant_price_per_item,
        SUM(number_of_boxes * box_weight) AS brutto_kg,
        SUM(box_length * box_width * box_height * number_of_boxes / 1000000) AS volume
    FROM (
        SELECT
            customer_request_id,
            CAST(expectedQuantity AS INT) AS qty,
            merchant_price_per_item / 1000000 AS merchant_price_per_item,
            box_length,
            box_width,
            box_height,
            box_weight,
            CAST(expectedQuantity AS INT) / IF(box_quantity = 0, NULL, box_quantity) AS number_of_boxes
        FROM {{ ref('fact_customer_requests_variants') }} AS v
        JOIN (
            SELECT
                deal_id
            FROM {{ ref('fact_deals_with_requests') }}
            WHERE self_service = 1
            GROUP BY deal_id
        ) AS d ON v.deal_id = d.deal_id
        WHERE CAST(expectedQuantity AS INT) > 0
    ) AS m
    GROUP BY customer_request_id
),

merchant AS (
    SELECT
        procurement_order_id,
        ROUND(SUM(price_per_item * original_qty) / SUM(original_qty), 2) AS price_per_item,
        SUM(original_qty) AS original_qty,
        SUM(qty) AS qty,
        SUM(number_of_boxes * box_weight) AS brutto_kg,
        SUM(box_length * box_width * box_height * number_of_boxes / 1000000) AS volume
    FROM (
        SELECT
            procurement_order_id,
            v_key AS variant_key,
            v_value._id AS variant_id,
            v_value.box.box.l AS box_length,
            v_value.box.box.w AS box_width,
            v_value.box.box.h AS box_height,
            v_value.box.box.weight AS box_weight,
            v_value.qty AS qty,
            v_value.originalQty AS original_qty,
            v_value.box.itemQuantityPerBox AS qty_per_box,
            v_value.qty / IF(v_value.box.itemQuantityPerBox = 0, NULL, v_value.box.itemQuantityPerBox) AS number_of_boxes,
            v_value.priceAmountPerItem / 1000000 AS price_per_item
        FROM {{ ref('procurement_orders') }}
        LATERAL VIEW explode(variants) AS v_key, v_value
    ) AS m
    GROUP BY procurement_order_id
),

warehouse_product AS (
    SELECT
        procurement_order_id,
        SUM(number_of_boxes * box_weight) AS box_brutto_kg,
        SUM(box_length * box_width * box_height * number_of_boxes / 1000000) AS box_volume
    FROM (
        SELECT
            procurement_order_id,
            list_entry.l AS box_length,
            list_entry.w AS box_width,
            list_entry.h AS box_height,
            list_entry.weight AS box_weight,
            list_entry.qty AS number_of_boxes,
            list_entry.qtyPerBox AS qty_per_box
        FROM {{ ref('procurement_orders') }}
        LATERAL VIEW explode(packaging.phases) AS phase_key, phase_array
        LATERAL VIEW explode(phase_array) AS list_entry
        WHERE list_entry.l IS NOT NULL
    ) AS m
    GROUP BY procurement_order_id
),

warehouse_pickup AS (
    SELECT
        procurement_order_id,
        SUM(number_of_boxes * pickup_box_weight) AS pickup_box_brutto_kg,
        SUM(pickup_box_length * pickup_box_width * pickup_box_height * number_of_boxes / 1000000) AS pickup_box_volume
    FROM (
        SELECT
            procurement_order_id,
            box.l AS pickup_box_length,
            box.w AS pickup_box_width,
            box.h AS pickup_box_height,
            box.weight AS pickup_box_weight,
            box.qty AS number_of_boxes,
            box.qtyPerBox AS qty_per_box
        FROM {{ ref('procurement_orders') }}
        LATERAL VIEW explode(pickup_order_box) AS box
        WHERE box.l > 0
    ) AS m
    GROUP BY procurement_order_id
),

warehouse AS (
    SELECT
        COALESCE(w.procurement_order_id, wp.procurement_order_id) AS procurement_order_id,
        COALESCE(w.box_brutto_kg, wp.pickup_box_brutto_kg) AS pickup_box_brutto_kg,
        COALESCE(w.box_volume, wp.pickup_box_volume) AS pickup_box_volume
    FROM warehouse_product AS w
    FULL OUTER JOIN warehouse_pickup AS wp
        ON w.procurement_order_id = wp.procurement_order_id
),

currency_rate AS (
    SELECT
        effective_date AS dt,
        currency_code,
        MAX(rate) AS rate
    FROM models.dim_pair_currency_rate
    WHERE currency_code_to = 'USD'
      AND effective_date >= '2023-01-01'
    GROUP BY effective_date, currency_code
)

SELECT
    r.procurement_order_id,

    -- SS data
    ss.qty AS request_qty_raw,
    ss.merchant_price_per_item AS merchant_price_per_item_raw,
    ss.brutto_kg AS request_weight_raw,
    ss.volume AS request_volume_raw,

    -- Merchant data
    m.price_per_item AS price_per_item_raw,
    m.price_per_item * cr.rate AS price_per_item_usd_raw,
    m.original_qty AS original_qty_raw,
    m.qty AS final_qty_raw,
    m.brutto_kg AS merchant_weight_raw,
    m.brutto_kg / IF(m.original_qty = 0, NULL, m.original_qty) AS merchant_weight_per_piece_raw,
    m.volume AS merchant_volume_raw,
    m.volume / IF(m.original_qty = 0, NULL, m.original_qty) AS merchant_volume_per_piece_raw,

    -- Warehouse data
    w.pickup_box_brutto_kg AS warehouse_weight_raw,
    w.pickup_box_brutto_kg / IF(m.qty = 0, NULL, m.qty) AS warehouse_weight_per_piece_raw,
    w.pickup_box_volume AS warehouse_volume_raw,
    w.pickup_box_volume / IF(m.qty = 0, NULL, m.qty) AS warehouse_volume_per_piece_raw,

    -- Comparison 1
    CASE
        WHEN (w.pickup_box_brutto_kg IS NOT NULL AND m.brutto_kg IS NOT NULL)
          OR (w.pickup_box_volume IS NOT NULL AND m.volume IS NOT NULL)
        THEN 1 ELSE 0
    END AS is_for_merchant_comparison,

    (w.pickup_box_brutto_kg / IF(m.qty = 0, NULL, m.qty) - m.brutto_kg / IF(m.original_qty = 0, NULL, m.original_qty))
        / (m.brutto_kg / IF(m.original_qty = 0, NULL, m.original_qty)) AS weight_diff_per_piece_pct_raw,

    (w.pickup_box_brutto_kg - m.brutto_kg) / m.brutto_kg AS weight_diff_pct_raw,
    (w.pickup_box_volume - m.volume) / m.volume AS volume_diff_pct_raw,

    (w.pickup_box_volume / IF(m.qty = 0, NULL, m.qty) - m.volume / IF(m.original_qty = 0, NULL, m.original_qty))
        / (m.volume / IF(m.original_qty = 0, NULL, m.original_qty)) AS volume_diff_per_piece_pct_raw,

    -- Comparison 2
    CASE
        WHEN (m.price_per_item IS NOT NULL AND ss.merchant_price_per_item IS NOT NULL)
          OR (m.brutto_kg IS NOT NULL AND ss.brutto_kg IS NOT NULL)
          OR (m.volume IS NOT NULL AND ss.volume IS NOT NULL)
        THEN 1 ELSE 0
    END AS is_for_confirmed_comparison,

    (m.price_per_item - ss.merchant_price_per_item) / ss.merchant_price_per_item AS merchant_price_diff_pct_raw,
    (m.brutto_kg - ss.brutto_kg) / ss.brutto_kg AS request_weight_diff_pct_raw,
    (m.volume - ss.volume) / ss.volume AS request_volume_diff_pct_raw

FROM {{ ref('procurement_orders') }} AS r
LEFT JOIN request AS ss ON r.customer_request_id = ss.customer_request_id
LEFT JOIN merchant AS m ON r.procurement_order_id = m.procurement_order_id
LEFT JOIN warehouse AS w ON r.procurement_order_id = w.procurement_order_id
LEFT JOIN currency_rate AS cr
    ON TO_DATE(r.created_ts) = cr.dt
    AND r.currency = cr.currency_code
ORDER BY 1, 2
