{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}
WITH interactions AS (
    SELECT distinct user_id, 
    utm_campaign,
    utm_source,
    utm_medium,
    source, 
    type,
    campaign
    from {{ ref('fact_interactions') }}
),

owners AS (
  SELECT admin_id,
    email
  FROM {{ ref('dim_user_admin') }}
),

customers AS (
  select customer_id as user_id, company_name 
  from {{ ref('sat_customer') }}
),

users AS (
  SELECT user_id,
  is_anonymous,
  created_ts_msk,
  update_ts_msk,
  pref_country,
  reject_reason,
  validation_status,
  owner_id,
  email as owner_email
  FROM {{ ref('dim_user') }}  u
  LEFT JOIN owners o ON o.admin_id = u.owner_id
  WHERE next_effective_ts_msk IS NULL
)

SELECT
  t.user_id,
  is_anonymous,
  created_ts_msk,
  update_ts_msk,
  pref_country,
  reject_reason,
  validation_status,
  owner_id,
  owner_email,
  utm_campaign,
  utm_source,
  utm_medium,
  source, 
  type,
  campaign,
  company_name,
  replace(replace(replace(replace(lower(company_name), '"', ''), ' ', ''), '«', ''), '»', '') AS company_name_for_join
FROM users t
LEFT JOIN interactions i on t.user_id = i.user_id
LEFT JOIN customers c ON t.user_id = c.user_id
