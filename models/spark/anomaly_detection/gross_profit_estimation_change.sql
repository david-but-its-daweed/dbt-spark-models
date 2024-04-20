{{
  config(
    materialized='table',
    alias='gross_profit_estimation_change',
    schema='models',
    file_format='parquet',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true'
    }
  )
}}

WITH main AS (
    SELECT
        cube_profit.t AS order_date_msk,
        cube_profit.country AS country_code,
        DATEDIFF(cube_profit.prediction_date, cube_profit.t) - 1 AS days_since_first_estimation,
        SUM(cube_profit.gmv_initial) AS gmv_initial,
        SUM(cube_profit.order_gross_profit_final_estimated) AS order_gross_profit_final_estimated, -- оценка GP
        MAX(
            IF(
                DATEDIFF(cube_profit.prediction_date, cube_profit.t) - 1 = 0,
                SUM(cube_profit.order_gross_profit_final_estimated),
                NULL
            )
        ) OVER (PARTITION BY cube_profit.t, cube_profit.country) AS order_gross_profit_final_estimated_first_estimation -- оценка GP на следующий день после даты создания заказа

    FROM {{ source('cube', 'profit') }} AS cube_profit
    INNER JOIN {{ ref('gold_countries') }} AS c
        ON cube_profit.country = c.country_code -- оставляю только страны
    WHERE cube_profit.t >= '2022-01-01'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
)

SELECT *
FROM main
WHERE
    TRUE
    AND days_since_first_estimation <= 180
    AND days_since_first_estimation >= 0
ORDER BY 1, 2, 3
