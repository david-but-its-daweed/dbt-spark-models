{{ config(
    schema='mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@aplotnikov',
      'bigquery_load': 'true'
    }
) }}

WITH product_docs AS (
    SELECT
        _id AS product_id,
        EXPLODE(docIds) AS doc_id
    FROM {{ source('mongo', 'product_merchant_document_product_links_daily_snapshot') }}
),

documents AS (
    SELECT
        _id AS doc_id,
        MILLIS_TO_TS(createdTimeMs) AS created_time,
        MILLIS_TO_TS(updatedTimeMs) AS updated_time,
        status,
        type
    FROM {{ source('mongo', 'core_merchant_documents_daily_snapshot') }}
)

SELECT
    documents.doc_id,
    product_docs.product_id,
    documents.created_time,
    documents.updated_time,
    documents.status,
    documents.type
FROM documents
LEFT JOIN product_docs USING (doc_id);