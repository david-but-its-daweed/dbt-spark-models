{{
  config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='insert_overwrite',
    on_schema_change='ignore',
    meta = {
        'model_owner' : '@general-analytics',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'date_msk',
    }
  )
}}

WITH base AS (
    SELECT
    -- Получаем дату события в МСК (UTC+3) из ObjectId (_id).
    -- Первые 8 символов ObjectId — это время в секундах в hex.
        TO_DATE(
            FROM_UTC_TIMESTAMP(
                CAST(CAST(CONV(SUBSTR(_id, 1, 8), 16, 10) AS BIGINT) AS TIMESTAMP),
                'Europe/Moscow'
            )
        ) AS date_msk,
        userId AS user_id,
        LOWER(type) AS points_redemption_type,
        distribution
    FROM {{ source('mongo', 'points_points_transactions_daily_snapshot') }}
),

-- Фильтруем только нужные события:
-- 1) тип транзакции purchase или cashout
-- 2) дата после 2024-01-01
-- 3) транзакция реально распределена (isDistributed = true)
filtered AS (
    SELECT
        date_msk,
        user_id,
        points_redemption_type,
        distribution
    FROM base
    WHERE
        points_redemption_type IN ('purchase', 'cashout')
        AND date_msk >= '2024-01-01' -- до этого distribution NULL
        AND date_msk < '{{ var("end_date_ymd") }}' -- исходные данные льются в UTC, заезжают лишние строки
        AND distribution.isDistributed = TRUE
),

main AS (
    SELECT
        f.date_msk,
        f.user_id,
        f.points_redemption_type,
        item.txType AS points_type,
        -- USD = amountUSD / 1_000_000 * mult.
        ROUND(SUM((CAST(item.amountUSD AS DOUBLE) / 1000000.0)), 3) AS points_redeemed_usd
    FROM filtered AS f
        -- Распаковываем distribution.byTx в строки.
        LATERAL VIEW EXPLODE(MAP_VALUES(f.distribution.byTx)) ev AS item
    GROUP BY 1, 2, 3, 4
)

SELECT
    main.date_msk,
    main.user_id,
    main.points_type,
    main.points_redemption_type,
    COALESCE(mapping.points_group, 'Other') AS points_group,
    main.points_redeemed_usd
FROM main
LEFT JOIN {{ ref('seed_points_groups_mapping') }} AS mapping USING (points_type)
ORDER BY 1