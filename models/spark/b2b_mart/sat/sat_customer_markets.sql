{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@ekutynina',
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT
    id,
    customer_id,
    market_id,
    TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM (
    SELECT
        concat_ws('-', customer_id, COALESCE(market_id, 0), utms) AS id,
        customer_id,
        update_ts_msk,
        market_id,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM (
        SELECT
            _id AS customer_id,
            utms,
            millis_to_ts_msk(utms) AS update_ts_msk,
            explode(markets) AS market_id,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to
        FROM {{ ref('scd2_customers_snapshot') }}
    )
)