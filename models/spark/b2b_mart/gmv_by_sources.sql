{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@amitiushkina',
      'priority_weight': '150',
      'bigquery_load': 'true'
    }
) }}

WITH with_row_id AS (
    SELECT
        *,
        MONOTONICALLY_INCREASING_ID() AS row_id
    FROM {{ ref('joom_pro_manual_orders') }}
),

latest_values AS (
    SELECT
        order_id,
        FIRST(order_created_date, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_order_created_date,
        FIRST(gmv_initial, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_gmv_initial,
        FIRST(initial_gross_profit, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_initial_gross_profit,
        FIRST(final_gross_profit, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_final_gross_profit
    FROM with_row_id
),

final_manual AS (
    SELECT DISTINCT
        order_id,
        last_order_created_date AS order_created_date,
        last_gmv_initial AS gmv_initial,
        last_initial_gross_profit AS initial_gross_profit,
        last_final_gross_profit AS final_gross_profit
    FROM latest_values
)

SELECT
    COALESCE(man.order_created_date, ord.t) AS t,
    ord.order_id,
    COALESCE(man.gmv_initial, ord.gmv_initial) AS gmv_initial,
    COALESCE(man.initial_gross_profit, ord.initial_gross_profit) AS initial_gross_profit,
    COALESCE(man.final_gross_profit, ord.final_gross_profit) AS final_gross_profit,
    ord.utm_campaign,
    ord.utm_source,
    ord.utm_medium,
    ord.source,
    ord.type,
    ord.campaign,
    ord.retention,
    ord.user_id,
    ord.country,
    ord.owner_email,
    ord.owner_role,
    ord.first_order,
    ord.client,
    ord.current_client
FROM {{ ref('gmv_by_sources_wo_filters') }} AS ord
LEFT JOIN final_manual AS man USING (order_id)
WHERE ord.order_id NOT IN (
    '657c58febbbdb8729dd7d39e', '658d3fc317e10341173c1f20', '659d3c4dddc19670cf999989',
    '65aa2a7ff11499f63900def9',
    '643d72b2acb1823e59eab7c5',
    '65e9c5b30d65cd6752992149',
    '663b87b734a8066e53968a66',
    '665f0ae08a8dac1a3ee66e1d',
    '666374d84c64a4aeb7c9aecb',
    '669ac6f8c980840fd9e5f2b7',
    '66a7aab79aa8c1361243e9d2', '66b388e4c5ad160d2d8de692', '66b3b9bc877fd3da349bb8ef',
    '66b1223b87628c73dce32443', '66c4f62e0b5bb457ba720a4f', '66ccad998d98f755caa68ad9',
    '66d20c48329656118385ec5a', '66d5f6bcace18af6ab5d4860', '66ce11498d98f755caa6923b',
    '66d9c9a1b1fcc25b92ce3f1d', '66da0c24182675e7411f2a28', '66e2f6c283d6709f08a84b03',
    '66ed74676f09959ab0fd0f12', '66ec20d252035764e906295a', '66f300b4e1ed52479981bd57',
    '66f5a143210729de9b82999e', '66fd455b0052353d1ce86fc3', '66fd5b223beac4b744f2c699',
    '670fa15a5b651d83f456bd54', '67090fcf4348d72dc52e7e89', '67100ba05b651d83f456c2bb',
    '671be7a60c96a194287a2766', '6720dff3ea371c645df9b48f', '67212806ea371c645df9b5a5'
)
