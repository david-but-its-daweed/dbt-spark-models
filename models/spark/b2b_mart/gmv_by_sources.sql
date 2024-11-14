{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'priority_weight': '150',
      'bigquery_load': 'true'
    }
) }}

SELECT
    t,
    order_id,
    gmv_initial,
    initial_gross_profit,
    final_gross_profit,
    utm_campaign,
    utm_source,
    utm_medium,
    source,
    type,
    campaign,
    retention,
    user_id,
    country,
    owner_email,
    owner_role,
    first_order,
    client,
    current_client
FROM {{ ref('gmv_by_sources_wo_filters') }}
WHERE order_id NOT IN ('657c58febbbdb8729dd7d39e', '658d3fc317e10341173c1f20', '659d3c4dddc19670cf999989', '65aa2a7ff11499f63900def9')
