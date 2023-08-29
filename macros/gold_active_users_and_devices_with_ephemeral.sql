{% macro gold_active_users_and_devices_with_ephemeral(device_or_user_id) %}



{% if  device_or_user_id == 'device_id' %}
    {% set naming_field = 'device' %}

    {{
        config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['date_msk', 'device_id'],
        partition_by=['date_msk'],
        incremental_predicates=["datediff(current_date(), TO_DATE(DBT_INTERNAL_DEST.date_msk)) < 183"],
        alias='active_devices_with_ephemeral',
        file_format='delta',
        schema='gold',
        meta = {
            'model_owner' : '@gusev'
        }
    )
    }}
{% elif device_or_user_id == 'user_id' %}
    {% set naming_field = 'user' %}

    {{
        config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['date_msk', 'device_id'],
        partition_by=['date_msk'],
        incremental_predicates=["datediff(current_date(), TO_DATE(DBT_INTERNAL_DEST.date_msk)) < 183"],
        alias='active_users_with_ephemeral',
        file_format='delta',
        schema='gold',
        meta = {
            'model_owner' : '@gusev'
        }
    )
    }}
{% endif %}


WITH
uniq_regions AS (
    SELECT * FROM {{ ref('gold_regions') }} WHERE is_uniq = TRUE
),

-- Orders info
orders_ext1 AS (
    SELECT * FROM {{ ref('gold_orders') }}
    where true
    {% if device_or_user_id == 'device_id' %}
        and order_date_msk >= '2018-04-15' -- до 2018-04-15 пустые device_id
    {% endif %}
    {% if is_incremental() or target.name != 'prod' %}
        and DATEDIFF(current_date(), order_date_msk) < 183
    {% endif %}
),

-- Orders info that require historical data and cant be calculated incrementally
orders_historical_info AS (
    select *
    from (
        select
            {{ device_or_user_id }},
            order_date_msk,
            SUM(COUNT(order_id)) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY order_date_msk) > 0 as is_payer
        FROM {{ ref('gold_orders') }}
        group by 1, 2
    )
    where true
    {% if device_or_user_id == 'device_id' %}
        and order_date_msk >= '2018-04-15' -- до 2018-04-15 пустые device_id
    {% endif %}
),


-- device Info that requires historical data and cant be calculated incrementally
active_devices_history_1 as (
    select
        {{ device_or_user_id }},
        date_msk,
        MIN(is_ephemeral) as is_ephemeral,
        {% if  device_or_user_id == 'device_id' %}
            IF(
                MIN(date_msk) OVER(PARTITION BY device_id) < FIRST_VALUE(TO_DATE(join_ts_msk)),
                MIN(date_msk) OVER(PARTITION BY device_id),
                FIRST_VALUE(TO_DATE(join_ts_msk))
            ) AS join_date_msk,
        {% elif  device_or_user_id == 'user_id' %}
            MIN(date_msk) OVER(PARTITION BY user_id) AS join_date_msk,
        {% endif %}
    FROM {{ source('mart', 'star_active_device') }}
    group by 1, 2
),

active_devices_history_2 as (
    -- добавляем предыдущий день активности
    SELECT
        *,
        DATEDIFF(date_msk, join_date_msk) AS {{ naming_field }}_lifetime,

        LAG(date_msk) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY date_msk) AS prev_date_msk, -- смотрим на предыдущий день активности
        LEAD(date_msk) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY date_msk) AS next_date_msk,  -- смотрим на следующий  день активности
        MIN(is_ephemeral) OVER(PARTITION BY {{ device_or_user_id }}) AS min_is_ephemeral,

        MAX(date_msk = join_date_msk) OVER(PARTITION BY {{ device_or_user_id }}, date_msk) AS is_new_{{ naming_field }}
    from active_devices_history_1
),

active_devices_history_3 as (
    -- добавляем группы по активности и времени жизни
    SELECT
        *,
        CASE
            WHEN is_new_{{ naming_field }} THEN 'new'
            WHEN prev_date_msk_lag BETWEEN 1 AND 28 THEN 'regular'
            ELSE 'reactivated'
        END AS previous_activity_device_group -- тип по последней активности устройства
    FROM (
        SELECT
            -- для пользователей без предыдущего дня, смотрим нет ли join date (иногда join date есть, а активности в этот день нет)
            *,
            IF(a_l = 0, {{ naming_field }}_lifetime, a_l) AS prev_date_msk_lag,
            DATEDIFF(next_date_msk, date_msk) AS next_date_msk_lag
        FROM (
            SELECT
                *,
                COALESCE(DATEDIFF(date_msk, prev_date_msk), 0) AS a_l -- заменяем нули
            FROM active_devices_history_2
        )
    )
),


active_devices_history_4 AS (
    SELECT
        *,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 1,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) > 0,
            NULL
        ) AS is_rd1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 3,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 3 FOLLOWING AND 3 FOLLOWING)) > 0,
            NULL
        ) AS is_rd3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 7 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rd7,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 14,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 14 FOLLOWING AND 14 FOLLOWING)) > 0,
            NULL
        ) AS is_rd14,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 1 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rw1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 14,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 8 FOLLOWING AND 14 FOLLOWING)) > 0,
            NULL
        ) AS is_rw2,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 21,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 15 FOLLOWING AND 21 FOLLOWING)) > 0,
            NULL
        ) AS is_rw3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 28,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 22 FOLLOWING AND 28 FOLLOWING)) > 0,
            NULL
        ) AS is_rw4,
        IF(
            DATEDIFF(CURRENT_DATE(), date_msk) >= 14,
            (next_date_msk_lag > 14 OR (next_date_msk_lag IS NULL AND DATEDIFF(CURRENT_DATE(), date_msk) >= 14)),
            NULL
        ) AS is_churned_14,
        IF(
            DATEDIFF(CURRENT_DATE(), date_msk) >= 28,
            (next_date_msk_lag > 28 OR (next_date_msk_lag IS NULL AND DATEDIFF(CURRENT_DATE(), date_msk) >= 28)),
            NULL
        ) AS is_churned_28
    FROM active_devices_history_3
),


-- Active devices info incremental
active_devices_ext1 AS (
    -- джоиним заказы к дням активности
    SELECT
        sd.{{ device_or_user_id }},
        sd.date_msk, -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(sd.real_user_id) AS real_user_id,
        FIRST_VALUE(UPPER(sd.country)) AS country_code,
        FIRST_VALUE(LOWER(sd.os_type)) AS platform,
        FIRST_VALUE(IF(sd.legal_entity = 'jmt', 'JMT', 'Joom')) AS legal_entity,
        FIRST_VALUE(UPPER(sd.language)) AS app_language,

        FIRST_VALUE(hd.date_msk = hd.join_date_msk) AS is_new_{{ naming_field }},
        FIRST_VALUE(hd.join_date_msk) AS join_date_msk,
        FIRST_VALUE(hd.{{ naming_field }}_lifetime) AS {{ naming_field }}_lifetime,
        FIRST_VALUE(hd.prev_date_msk_lag) AS prev_date_msk_lag,
        FIRST_VALUE(hd.next_date_msk_lag) AS next_date_msk_lag,
        FIRST_VALUE(hd.previous_activity_device_group) AS previous_activity_device_group,
        FIRST_VALUE(hd.min_is_ephemeral) AS is_ephemeral_{{ naming_field }},

        COALESCE(SUM(go.gmv_initial), 0) AS gmv_per_day_initial,
        COALESCE(SUM(go.gmv_final), 0) AS gmv_per_day_final,
        COALESCE(SUM(go.order_gross_profit_final_estimated), 0) AS order_gross_profit_per_day_final_estimated,
        COALESCE(SUM(go.order_gross_profit_final), 0) AS order_gross_profit_per_day_final,
        COALESCE(SUM(go.ecgp_initial), 0) AS ecgp_per_day_initial,
        COALESCE(SUM(go.ecgp_final), 0) AS ecgp_per_day_final,
        COUNT(go.order_id) AS number_of_orders,
        MAX(hgo.is_payer) > 0 AS is_payer,
        COUNT(go.order_id) > 0 AS is_converted,

        FIRST_VALUE(hd.is_rd1) as is_rd1,
        FIRST_VALUE(hd.is_rd3) as is_rd3,
        FIRST_VALUE(hd.is_rd7) as is_rd7,
        FIRST_VALUE(hd.is_rd14) as is_rd14,

        FIRST_VALUE(hd.is_rw1) as is_rw1,
        FIRST_VALUE(hd.is_rw2) as is_rw2,
        FIRST_VALUE(hd.is_rw3) as is_rw3,
        FIRST_VALUE(hd.is_rw4) as is_rw4,

        FIRST_VALUE(hd.is_churned_14) as is_churned_14,
        FIRST_VALUE(hd.is_churned_28) as is_churned_28
    FROM {{ source("mart", "star_active_device") }} AS sd
    LEFT JOIN orders_ext1 AS go ON sd.{{ device_or_user_id }} = go.{{ device_or_user_id }} AND sd.date_msk = go.order_date_msk
    LEFT JOIN orders_historical_info AS hgo ON sd.{{ device_or_user_id }} = hgo.{{ device_or_user_id }} AND sd.date_msk = hgo.order_date_msk
    LEFT JOIN active_devices_history_4 as hd ON sd.{{ device_or_user_id }} = hd.{{ device_or_user_id }} AND sd.date_msk = hd.date_msk
    {% if is_incremental() or target.name != 'prod' %}
        where DATEDIFF(current_date(), sd.date_msk) < 183
    {% endif %}

    GROUP BY 1, 2
),


active_devices_ext2 AS (
    -- добавляем основной регион
    SELECT
        a.*,
        c.top_country_code,
        COALESCE(b.region_name, 'Other') AS region_name
    FROM active_devices_ext1 AS a
    LEFT JOIN uniq_regions AS b USING(country_code)
    LEFT JOIN {{ ref('gold_countries') }} c USING(country_code)
),

active_devices_ext3 AS (
    -- добавляем сегмент пользователя
    SELECT
        a.*,
        COALESCE(b.user_segment, 'Non-buyers') AS real_user_segment
    FROM active_devices_ext2 AS a
    LEFT JOIN {{ ref('user_segments') }} AS b
        ON
            a.real_user_id = b.real_user_id
            AND a.date_msk >= TO_DATE(b.effective_ts)
            AND a.date_msk <= TO_DATE(b.next_effective_ts)
)

SELECT
    date_msk,

    {{ device_or_user_id }},
    real_user_id,

    country_code,
    top_country_code,
    region_name,
    app_language,
    platform,
    legal_entity,
    join_date_msk,
    real_user_segment,
    is_new_{{ naming_field }},
    is_ephemeral_{{ naming_field }},
    {{ naming_field }}_lifetime,

    previous_activity_device_group,
    prev_date_msk_lag,
    next_date_msk_lag,
    gmv_per_day_initial,
    gmv_per_day_final,
    order_gross_profit_per_day_final_estimated,
    order_gross_profit_per_day_final,
    ecgp_per_day_initial,
    ecgp_per_day_final,
    number_of_orders,

    is_payer,
    is_converted,
    is_rd1,
    is_rd3,
    is_rd7,
    is_rd14,
    is_rw1,
    is_rw2,
    is_rw3,
    is_rw4,
    is_churned_14,
    is_churned_28
FROM active_devices_ext3

{% endmacro %}
