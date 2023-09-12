{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'false'
    }
) }}


SELECT concat_ws('-', customer_id, COALESCE(schema_id,0), utms) as id,
    customer_id,
    update_ts_msk,
    schema_id,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM (
    SELECT
        _id                          AS customer_id,
       utms,
       millis_to_ts_msk(utms)       AS update_ts_msk,
       explode(schemes)             AS schema_id,
       dbt_scd_id,
       dbt_updated_at,
       dbt_valid_from,
       dbt_valid_to
    FROM {{ ref('scd2_customers_snapshot') }}
 )
