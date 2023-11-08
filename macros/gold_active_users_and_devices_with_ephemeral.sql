{% macro gold_active_users_and_devices_with_ephemeral(device_or_user_id) %}



{% if  device_or_user_id == 'device_id' %}
    {% set naming_field = 'device' %}

    {{
        config(
        materialized='incremental',
        alias='active_devices_with_ephemeral',
        file_format='parquet',
        schema='gold',
        meta = {
            'model_owner' : '@gusev',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_upload_horizon_days': '230',
            'bigquery_override_dataset_id': 'gold',
            'priority_weight': '1000',
        },
        incremental_strategy='insert_overwrite',
        partition_by=['month'],
    )
    }}
{% elif device_or_user_id == 'user_id' %}
    {% set naming_field = 'user' %}

    {{
        config(
        materialized='incremental',
        alias='active_users_with_ephemeral',
        file_format='parquet',
        schema='gold',
        meta = {
            'model_owner' : '@gusev',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_upload_horizon_days': '230',
            'bigquery_override_dataset_id': 'gold',
            'priority_weight': '1000',
        },
        incremental_strategy='insert_overwrite',
        partition_by=['month'],
    )
    }}
{% endif %}


WITH
uniq_regions AS (
    SELECT * FROM {{ ref('gold_regions') }} WHERE is_uniq = TRUE
),

first_order_dates AS (
    SELECT
        {{ device_or_user_id }},
        min(order_date_msk) as dt
    FROM {{ ref('gold_orders') }}
    GROUP BY 1
),

orders_ext1 AS (
    SELECT
        {{ device_or_user_id }},
        order_date_msk as date_msk,
        SUM(gmv_initial) AS gmv_per_day_initial,
        SUM(gmv_final) AS gmv_per_day_final,
        SUM(order_gross_profit_final_estimated) AS order_gross_profit_per_day_final_estimated,
        SUM(order_gross_profit_final) AS order_gross_profit_per_day_final,
        SUM(ecgp_initial) AS ecgp_per_day_initial,
        SUM(ecgp_final) AS ecgp_per_day_final,
        COUNT(order_id) AS number_of_orders,
        COUNT(order_id) > 0 AS is_converted
    FROM {{ ref('gold_orders') }}

    {% if is_incremental() %}
        WHERE month >= trunc(date'{{ var("start_date_ymd") }}' - interval 200 days, 'MM')
    {% elif device_or_user_id == 'device_id' %}
        WHERE order_date_msk >= '2018-04-15' -- до 2018-04-15 пустые device_id
    {% endif %}
    GROUP BY 1, 2
),

active_devices_ext0 as (
    -- добавляем предыдущий день активности и другие св-ва, для которых требуется вся история
    select
        {{ device_or_user_id }},
        day,
        LAG(day) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY day) AS prev_date_msk, -- смотрим на предыдущий день активности
        LEAD(day) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY day) AS next_date_msk,  -- смотрим на следующий  день активности
        MIN(is_ephemeral) OVER(PARTITION BY {{ device_or_user_id }}) AS min_is_ephemeral

    {% if device_or_user_id == 'user_id' %}
        FROM {{ ref('active_users') }}
    {% else %}
        FROM {{ ref('active_devices') }}
    {% endif %}
),

active_devices_ext1 AS (
    SELECT
        main.{{ device_or_user_id }},
        main.day as date_msk,
        main.real_user_id,
        main.join_day as join_date_msk,
        main.legal_entity,
        {% if device_or_user_id == 'device_id' %}
            main.app_entity,
        {% endif %}
        main.country as country_code,
        main.app_language,
        main.platform,
        main.os_version,
        main.app_version,
        main.is_ephemeral,
        DATEDIFF(main.day, main.join_day) AS {{ naming_field }}_lifetime,

        aux.prev_date_msk,
        aux.next_date_msk,
        aux.min_is_ephemeral,

        main.day = main.join_day AS is_new_{{ naming_field }}

    {% if device_or_user_id == 'user_id' %}
        FROM {{ ref('active_users') }} as main
    {% else %}
        FROM {{ ref('active_devices') }} as main
    {% endif %}
    JOIN active_devices_ext0 as aux USING ({{ device_or_user_id}}, day)

    {% if is_incremental() %}
        WHERE month >= trunc(date'{{ var("start_date_ymd") }}' - interval 200 days, 'MM')
    {% endif %}
),

active_devices_ext2 AS (
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
            FROM active_devices_ext1
        )
    )
),

active_devices_ext3 AS (
    -- джоиним заказы к дням активности
    SELECT
        a.{{ device_or_user_id }},
        a.date_msk,
        a.real_user_id,
        a.country_code,
        a.platform,
        a.legal_entity,
        {% if device_or_user_id == 'device_id' %}
            a.app_entity,
        {% endif %}
        a.app_language,
        a.is_new_{{ naming_field }},
        a.join_date_msk,
        a.{{ naming_field }}_lifetime,
        a.prev_date_msk_lag,
        a.next_date_msk_lag,
        a.previous_activity_device_group,
        a.min_is_ephemeral AS is_ephemeral_{{ naming_field }},

        COALESCE(b.gmv_per_day_initial, 0) AS gmv_per_day_initial,
        COALESCE(b.gmv_per_day_final, 0) AS gmv_per_day_final,
        COALESCE(b.order_gross_profit_per_day_final_estimated, 0) AS order_gross_profit_per_day_final_estimated,
        COALESCE(b.order_gross_profit_per_day_final, 0) AS order_gross_profit_per_day_final,
        COALESCE(b.ecgp_per_day_initial, 0) AS ecgp_per_day_initial,
        COALESCE(b.ecgp_per_day_final, 0) AS ecgp_per_day_final,
        COALESCE(b.number_of_orders, 0) AS number_of_orders,

        COALESCE(a.date_msk >= f.dt, false) AS is_payer,

        COALESCE(b.is_converted, false) as is_converted
    FROM active_devices_ext2 AS a
    LEFT JOIN orders_ext1 AS b USING ({{ device_or_user_id }}, date_msk)
    LEFT JOIN first_order_dates f USING ({{ device_or_user_id }})
),

active_devices_ext4 AS (
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
    FROM active_devices_ext3
),

active_devices_ext5 AS (
    -- добавляем основной регион
    SELECT
        a.*,
        c.top_country_code,
        COALESCE(b.region_name, 'Other') AS region_name
    FROM active_devices_ext4 AS a
    LEFT JOIN uniq_regions AS b USING(country_code)
    LEFT JOIN {{ ref('gold_countries') }} c USING(country_code)
),

active_devices_ext6 AS (
    -- добавляем сегмент пользователя
    SELECT
        a.*,
        COALESCE(b.user_segment, 'Non-buyers') AS real_user_segment
    FROM active_devices_ext5 AS a
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
    {% if device_or_user_id == 'device_id' %}
        app_entity,
    {% endif %}
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
    is_churned_28,
    trunc(date_msk, 'MM') as month
FROM active_devices_ext6

{% endmacro %}
