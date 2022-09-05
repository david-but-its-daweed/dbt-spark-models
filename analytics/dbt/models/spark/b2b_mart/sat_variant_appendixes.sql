{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT variant_id,
       product_id,
       minimum_box_qnty,
       medium_box_qnty,
       maximum_box_qnty,
       minimum_box_hight,
       medium_box_hight,
       maximum_box_hight,
       minimum_box_width,
       medium_box_width,
       maximum_box_width,
       minimum_box_length,
       medium_box_length,
       maximum_box_length,
       price_min_qnty,
       price_medium_qnty,
       price_max_qnty,
       article,
       trademark,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_variant_appendixes') }} t