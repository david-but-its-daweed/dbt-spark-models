{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'false'
    }
) }}

SELECT _id AS rfq_request_id,
    millis_to_ts_msk(ctms) AS created_ts_msk,
    descr AS description,
    name,
    oid AS order_id,
    plnk AS link,
    price.amount AS price,
    price.ccy AS ccy,
    qty AS qty,
    status,
    millis_to_ts_msk(stms) sent_ts_msk,
    variants,
    coalesce(isTop, FALSE) as top_rfq,
    millis_to_ts_msk(utms) AS update_ts_msk,
    rfq.categories[0] as category_id,
    category_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('scd2_rfq_request_snapshot') }} rfq
left join 
(
select category_id, name as category_name
    from {{ source('mart', 'category_levels') }}
) cat on rfq.categories[0] = cat.category_id
