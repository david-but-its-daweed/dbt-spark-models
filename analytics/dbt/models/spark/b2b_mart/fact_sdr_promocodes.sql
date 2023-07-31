{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-03-01']
    }
) }}


WITH customers AS (
    SELECT
        fc.user_id,
        fc.country,
        fc.owner_id,
        fc.owner_email,
        fc.owner_role,
        fc.last_name,
        fc.first_name,
        fc.company_name,
        du.is_partner,
        fc.partner_type,
        fc.partner_source
    FROM {{ ref('fact_customers') }} AS fc
    LEFT JOIN {{ ref('dim_user') }} AS du ON fc.user_id = du.user_id
    WHERE du.next_effective_ts_msk IS NULL
),

attr AS (
    SELECT DISTINCT
        type,
        source,
        campaign,
        user_id
    FROM {{ ref('fact_attribution_interaction') }}
    WHERE last_interaction_type
)


SELECT DISTINCT
    _id AS promocode_id,
    code,
    millis_to_ts_msk(ctms) AS created_time_msk,
    isActive AS is_active,
    ownerId AS promocode_owner_id,
    country,
    owner_id,
    owner_email,
    owner_role,
    last_name,
    first_name,
    company_name,
    is_partner,
    partner_type,
    partner_source,
    type,
    source,
    campaign,
    to_date(CURRENT_DATE()) - INTERVAL 1 DAY AS partition_date_msk,
    to_date(CURRENT_DATE()) - INTERVAL 1 DAY AS day
FROM customers AS c
LEFT JOIN
(
select *, 
    to_date(CURRENT_DATE()) - INTERVAL 1 DAY AS partition_date_msk
from {{ ref('scd2_promocodes_snapshot') }}
) p on p.ownerId = c.user_id
left join attr a on a.user_id = p.ownerId
where partition_date_msk is null or (
    date(millis_to_ts_msk(ctms)) <= partition_date_msk and 
    partition_date_msk >=  date(dbt_valid_from) and (partition_date_msk < date(dbt_valid_to) or dbt_valid_to is null)
    )
