{{
  config(
    meta = {
      'model_owner' : '@gusev'
    },
    materialized='incremental',
    alias='active_users',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['day', 'user_id'],
    partition_by=['month'],
    incremental_predicates=["DBT_INTERNAL_DEST.month >= trunc(current_date() - interval 300 days, 'MM')"]
  )
}}


with user_info as (
    select
        user_id,
        date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(UPPER(country)) AS country,
        FIRST_VALUE(LOWER(os_type)) AS platform,
        FIRST_VALUE(os_version) AS os_version,
        FIRST_VALUE(app_version) AS app_version,
        MIN(ephemeral) AS is_ephemeral
    from {{ source('mart', 'star_active_device')}}
    {% if is_incremental() %}
         where date_msk >= date'{{ var("start_date_ymd") }}' - interval 270 days
    {% endif %}
    group by 1, 2
),

join_dates as (
    select
        user_id,
        min(date_msk) as join_day
    from {{ source('mart', 'star_active_device')}}
    group by 1
)

SELECT
    u.user_id,
    u.day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
    jd.join_day,
    u.country,
    u.platform,
    u.os_version,
    u.app_version,
    u.is_ephemeral,
    u.day = jd.join_day as is_new_user,
    trunc(u.day, 'MM') as month
from user_info as u
join join_dates as jd using(user_id)
