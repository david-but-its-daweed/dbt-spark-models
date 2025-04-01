{{
  config(
    meta = {
      'model_owner' : '@itangaev',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'day',
      'priority_weight': '1000',
      'bigquery_upload_horizon_days': '230',
      'bigquery_check_counts': 'false'
    },
    materialized='table',
    file_format='delta',
  )
}}


WITH userPointsTransactions AS (
    -- Интересны только транзакции юзера за последний год - более ранняя детализация не важна
    SELECT
        user_id,
        point_transaction_type,
        CASE
            WHEN point_transaction_type IN ("purchase", "expiration") THEN point_transaction_type
            WHEN point_transaction_type LIKE 'admin%' AND point_usd < 0 THEN "expiration"
            WHEN (point_transaction_type LIKE 'admin%' or point_transaction_type in ("revenueShareExternalLink", "revenueShareSocial")) AND point_usd > 0 THEN "Marketing_bloggers"
            WHEN point_usd < 0 THEN "other_spends"
            ELSE point_transaction_group
        END AS transaction_group,
        day AS event_day,
        point_usd AS points_usd,
        IF(point_usd > 0, point_usd, 0) AS points_usd_input,
        IF(point_usd < 0, point_usd, 0) AS points_usd_output,
        value,
        ccy,
        mult
    FROM {{ ref('user_points_transactions') }}
    WHERE day >= ADD_MONTHS(CURRENT_DATE, -24)
),

rawPointBalance AS (
    -- Из истории забираем информацию о тотал балансе баллов у пользователя
    SELECT
        payload.userId AS user_id,
        payload.pointsBalance.value AS value,
        payload.pointsBalance.ccy AS ccy,
        payload.pointsBalance.mult AS mult,
        payload.txTime,
        DATE(FROM_UNIXTIME(payload.txTime / 1000 + 3 * 3600)) AS dt
    FROM {{ source('mart', 'unclassified_events') }}
    WHERE type = 'pointsBalanceSync'
        AND DATE(FROM_UNIXTIME(payload.txTime / 1000 + 3 * 3600)) >= ADD_MONTHS(CURRENT_DATE, -36)
),

user_ccy AS (
    SELECT
        user_id,
        ccy AS most_frequent_ccy
    FROM (
        SELECT
            user_id,
            ccy,
            COUNT(*) AS transaction_count,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY COUNT(*) DESC, ccy ASC  -- Сортировка: сначала по убыванию количества, затем по алфавиту (стабильность)
            ) AS rn
        FROM userPointsTransactions
        GROUP BY user_id, ccy
    )
    WHERE rn = 1
),

userPointsTransactionsInLocalCurrency AS (
    SELECT
        t1.*,
        CASE
            WHEN t1.ccy = t2.most_frequent_ccy THEN t1.value / t1.mult * SIGN(points_usd)  -- так как value всегда положительное
            ELSE t1.value / t1.mult * t3.rate / t4.rate * SIGN(points_usd)
        END AS points_local,
        t2.most_frequent_ccy as most_frequent_ccy
    FROM userPointsTransactions t1
    JOIN user_ccy t2 ON t1.user_id = t2.user_id
    JOIN {{ source('mart', 'dim_currency_rate') }} t3
        ON t1.ccy = t3.currency_code
        AND t1.event_day = t3.effective_date
    JOIN {{ source('mart', 'dim_currency_rate') }} t4
        ON t2.most_frequent_ccy = t4.currency_code
        AND t1.event_day = t4.effective_date
),

rawPointBalanceInLocalCurrency AS (
    SELECT
        t1.*,
        CASE
            WHEN t1.ccy = t2.most_frequent_ccy THEN t1.value / t1.mult  -- а здесь на знак домножать не надо, т.к. баланс всегда неотрицательный
            ELSE t1.value / t1.mult / t3.rate * t4.rate
        END AS points_local
    FROM rawPointBalance t1
    JOIN user_ccy t2 ON t1.user_id = t2.user_id
    JOIN {{ source('mart', 'dim_currency_rate') }} t3
        ON t1.ccy = t3.currency_code
        AND t1.dt = t3.effective_date
    JOIN {{ source('mart', 'dim_currency_rate') }} t4
        ON t2.most_frequent_ccy = t4.currency_code
        AND t1.dt = t4.effective_date
),

balance_current AS (
    SELECT * FROM (
        SELECT
            user_id,
            points_local,
            ccy,
            mult,
            dt,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) AS rn_desc
        FROM rawPointBalanceInLocalCurrency
    )
    WHERE rn_desc = 1
),

balance_historical AS (
    SELECT * FROM (
        SELECT
            user_id,
            points_local,
            ccy,
            mult,
            dt,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) AS rn_desc
        FROM (SELECT * FROM rawPointBalanceInLocalCurrency WHERE dt <= ADD_MONTHS(CURRENT_DATE, -24) )  -- находим баланс 2 года назад
    )
    WHERE rn_desc = 1
),

transactions_with_base as (  -- приклеиваем базу к транзакциям
    select
        a.*,
        IF(a.points_local > 0, a.points_local, 0) AS points_local_input,
        IF(a.points_local < 0, a.points_local, 0) AS points_local_output,

        coalesce(history.points_local, 0) as value_one_year_ago_local,
        coalesce(current.points_local, 0) as value_current_local
    from userPointsTransactionsInLocalCurrency a
    left join balance_historical history
    on a.user_id = history.user_id
    left join balance_current current
    on a.user_id = current.user_id
    ORDER BY user_id, event_day, points_usd DESC
),

transactions_with_base_and_cumulatives AS (
    -- считаем накопленный баланс начислений и списаний баллов после event_day в разбивке по основным группам списаний
    SELECT
        transactions_with_base.*,
        value_one_year_ago_local as base_balance_local,
        value_current_local as balance_current_local,
        SUM(points_local_input)
            OVER (PARTITION BY user_id ORDER BY event_day, points_usd DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)  -- ORDER BY points_usd DESC - костыль, спасающий в ситуациях, когда у юзера в один день были транзакции и начисления, и списания: считаем, что тогда скорее всего он сначала получил баллы, а потом тут же потратил (вероятность обратного крайне мала; и даже если он на самом деле сначала потратил X, а потом получил в этот же день Y, наша модель лишь наврет немного в аллокации баллов Y).
            AS total_income_preceding,
        SUM(points_local_output)
            OVER (PARTITION BY user_id ORDER BY event_day, points_usd DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
            AS total_outcome_preceding,

        SUM(points_local_output)
            OVER (PARTITION BY user_id ORDER BY event_day, points_usd DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
            AS total_outcome_following,
        SUM(CASE WHEN transaction_group = 'purchase' THEN points_local_output ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY event_day, points_usd DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
            AS purchase_outcome_following,
        SUM(CASE WHEN transaction_group = 'expiration' THEN points_local_output ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY event_day, points_usd DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
            AS expiration_outcome_following,
        SUM(CASE WHEN transaction_group not in ('expiration', 'purchase') THEN points_local_output ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY event_day, points_usd DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
            AS other_outcome_following
    FROM transactions_with_base
),

transactions_with_real_costs AS (
    -- добавляем расчет распределенных костов
    SELECT
        a.*,

        coalesce(GREATEST(0, LEAST(1, (ABS(total_outcome_following) + ABS(total_outcome_preceding) - base_balance_local - total_income_preceding ) / points_local_input)) * purchase_outcome_following / total_outcome_following, 0) as purchased_rate,  -- если списаний в будущем было больше, чем был баланс до начисления текущей транзакции X, то значит какую-то часть баллов X юзер потратит. Какую именно часть - зависит от того, насколько списаний в будущем больше, чем баланс. А текущий баланс = баланс год назад + все предыдущие начисления - все предыдущие списания
        coalesce(GREATEST(0, LEAST(1, (ABS(total_outcome_following) + ABS(total_outcome_preceding) - base_balance_local - total_income_preceding ) / points_local_input)) * expiration_outcome_following / total_outcome_following, 0) as burned_rate,
        coalesce(GREATEST(0, LEAST(1, (ABS(total_outcome_following) + ABS(total_outcome_preceding) - base_balance_local - total_income_preceding ) / points_local_input)) * other_outcome_following / total_outcome_following, 0) as other_outcome_rate,
        1 - burned_rate - purchased_rate - other_outcome_rate as remained_rate,

        points_local_input * burned_rate as total_burned,
        points_local_input * purchased_rate as total_purchased,
        points_local_input * remained_rate as total_remained,
        points_local_input * other_outcome_rate as total_other   -- поле для мониторинга того, что ничего нигде не просыпается


FROM transactions_with_base_and_cumulatives a
),

transactions_with_real_costs_and_currency_rates as (
    SELECT
        t.*,
        c1.rate / 1000000.0 as local_ccy_to_usd_rate_history,
        c2.rate / 1000000.0 as local_ccy_to_usd_rate_current
        FROM transactions_with_real_costs t
    JOIN {{ source('mart', 'dim_currency_rate') }} c1
        ON t.most_frequent_ccy = c1.currency_code
        AND t.event_day = c1.effective_date
    JOIN (
        SELECT *
        FROM {{ source('mart', 'dim_currency_rate') }}
        WHERE next_effective_date > "2200-01-01"
    ) c2
    ON t.most_frequent_ccy = c2.currency_code
)

SELECT * FROM transactions_with_real_costs_and_currency_rates
