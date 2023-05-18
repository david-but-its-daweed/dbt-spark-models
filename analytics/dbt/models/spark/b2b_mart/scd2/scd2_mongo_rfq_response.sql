{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'false'
    }
) }}


SELECT _id AS order_rfq_response_id,
    size(docIds) AS documents_attached,
    comment,
    millis_to_ts_msk(ctms) AS created_ts_msk,
    dscr AS description,
    mId AS merchant_id,
    pId AS product_id,
    rejectReason AS reject_reason,
    rejectReasonGroup AS reject_reason_group,
    productInStock as product_in_stock,
    manufacturingDays as manufacturing_days,
    rfqid AS rfq_request_id,
    sent,
    status,
    millis_to_ts_msk(utms) AS update_ts_msk
FROM {{ ref('scd2_rfq_response_snapshot') }}
