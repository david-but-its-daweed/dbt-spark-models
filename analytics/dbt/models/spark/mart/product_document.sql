{{ config(
    schema='mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}
with product_docs as (
    select
        _id as product_id,
        explode(docIds) as doc_id
    from {{ source('mongo', 'product_merchant_document_product_links_daily_snapshot') }} 
),
documetns as (
    select
        _id as doc_id,
        millis_to_ts(createdTimeMs) created_time,
        millis_to_ts(updatedTimeMs) updated_time,
        status,
        type
    from {{ source('mongo', 'core_merchant_documents_daily_snapshot') }}
)

select doc_id,
       product_id,
       created_time,
       updated_time,
       status,
       type
from documetns
         left join product_docs using (doc_id)