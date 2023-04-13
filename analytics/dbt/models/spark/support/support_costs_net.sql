{{ config(
     schema='support',
     materialized='table',
     partition_by=['date'],
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'date',
       'alerts_channel': "#olc_dbt_alerts",
       'bigquery_fail_on_missing_partitions': 'false'
     }
 ) }}
 
WITH t AS (
SELECT
    a.email,
    b.name AS sb_command,
    CASE WHEN a.currency = 'Rouble' THEN 'RUB'
         WHEN a.currency = 'Dollar' THEN 'USD'
         WHEN a.currency = 'Euro' THEN 'EUR' ELSE 'RUB' END AS currency,
    t.`date`,
    SUM(t.salary + t.fix) AS total_payment
FROM {{ source('support', 'shiftbase_rosters_snapshot_2023') }} AS t
JOIN {{ source('support', 'shiftbase_users') }} AS a ON t.user_id = a.id and t.partition_date = a.partition_date
JOIN {{ source('support', 'shiftbase_teams') }} AS b ON t.team_id = b.id and t.partition_date = b.partition_date
WHERE t.partition_date = (SELECT MAX(partition_date) FROM {{ source('support', 'shiftbase_rosters_snapshot_2023') }})
GROUP BY 1, 2, 3, 4
),

sb AS (
SELECT
    t.email,
    t.sb_command,
    t.`date`,
    CASE WHEN t.currency = 'RUB' THEN t.total_payment / a.rate --переводим рубли в доллары
         WHEN t.currency = 'EUR' THEN t.total_payment * (b.rate / a.rate) --переводим евро в доллары отношением курса евро к рублю к курса доллара к рублю
         ELSE t.total_payment END AS total_payment_converted,
    1 AS all_resolved_tickets,
    t.total_payment
FROM t
LEFT JOIN {{ source('support', 'cbr_currency_rate') }} AS a ON (CASE WHEN t.`date` < '2023-04-07' THEN '2023-04-07' ELSE t.`date` END) = a.partition_date AND a.to = 'USD' --с '2023-04-07' есть данные о курсах валют
LEFT JOIN {{ source('support', 'cbr_currency_rate') }} AS b ON  (CASE WHEN t.`date` < '2023-04-07' THEN '2023-04-07' ELSE t.`date` END) = b.partition_date AND b.to = 'EUR'
),

base AS (
  SELECT
    b.email,
    DATE(t.resolution_ticket_ts_msk) AS `date`,
    t.ticket_id,
    t.first_queue_not_limbo,
    COALESCE(t.country, 'Unknown') AS country,
    b.sb_command,
    b.total_payment_converted
  FROM
    {{ ref('support_mart_ticket_id_ext') }} AS t
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS a ON t.last_agent = a._id
    RIGHT JOIN sb AS b ON a.email = b.email --LEFT
        AND DATE(t.resolution_ticket_ts_msk) = b.`date`
  WHERE
    DATE(t.resolution_ticket_ts_msk) >= '2023-01-01'
    AND is_closed = 'yes'
    AND b.email IS NOT NULL
),

pre_overall AS (
  SELECT
    email,
    `date`,
    COUNT(DISTINCT ticket_id) AS all_resolved_tickets
  FROM
    base
  GROUP BY
    1,
    2
),

overall AS (
    SELECT
        t.email,
        t.sb_command,
        t.`date`,
        t.total_payment_converted,
        GREATEST(t.all_resolved_tickets, a.all_resolved_tickets) AS all_resolved_tickets
    FROM sb AS t
    LEFT JOIN pre_overall AS a ON t.email = a.email
        AND t.`date` = a.`date`
),

prebase AS (
  SELECT
    a.email,
    a.sb_command,
    t.first_queue_not_limbo,
    t.country,
    a.`date`,
    a.total_payment_converted,
    a.all_resolved_tickets,
    COUNT(DISTINCT t.ticket_id) AS resolved_tickets
  FROM
    base AS t
    RIGHT JOIN overall AS a ON t.email = a.email
        AND t.`date` = a.`date`
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
),

all_final AS (
  SELECT
    *,
    total_payment_converted * (
      COALESCE((CASE WHEN resolved_tickets = 0 THEN 1 ELSE resolved_tickets END) / all_resolved_tickets, 1)
    ) AS payment_usd
  FROM
    prebase
)


SELECT
  t.`date`,
  t.sb_command,
  t.first_queue_not_limbo AS babylone_queue,
  t.country,
  SUM(t.payment_usd) AS total_payment_usd
FROM
  all_final AS t
GROUP BY
  1,
  2,
  3,
  4
