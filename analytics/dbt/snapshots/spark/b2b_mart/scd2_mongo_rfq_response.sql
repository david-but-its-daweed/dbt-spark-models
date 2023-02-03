{% snapshot scd2_mongo_rfq_response %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='order_rfq_response_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}
SELECT _id AS order_rfq_response_id,
    size(docIds) AS documents_attached,
    comment,
    millis_to_ts_msk(ctms) AS created_ts_msk,
    dscr AS description,
    mId AS merchant_id,
    pId AS product_id,
    rejectReason AS reject_reason,
    rfqid AS rfq_request_id,
    sent,
    status,
    millis_to_ts_msk(utms) AS update_ts_msk
FROM {{ source('mongo', 'b2b_core_rfq_response_daily_snapshot') }}
{% endsnapshot %}
