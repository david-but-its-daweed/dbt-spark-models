{% snapshot scd2_mongo_product_certification_states %}

{{
    config(
    meta = {
      'model_owner' : '@daweed'
    },
    target_schema='b2b_mart',
    unique_key='product_id',

    strategy='timestamp',
    updated_at='updated_ts_msk',
    file_format='delta'
    )
}}

SELECT
    _id AS product_certification_id,
    pId AS product_id,

    CASE
        WHEN cs['BR'].cert.c = 0 THEN NULL
        WHEN cs['BR'].cert.c = 1 THEN True
        WHEN cs['BR'].cert.c = 2 THEN False
    END AS has_certification,
    cs['BR'].cert.r AS certification_reason,

    CASE
        WHEN cs['BR'].reg.l = 0 THEN NULL
        WHEN cs['BR'].reg.l = 1 THEN True
        WHEN cs['BR'].reg.l = 2 THEN False
    END AS has_registration,
    cs['BR'].reg.r AS registration_reason,

    cs['BR'].reg.ma AS missing_agencies,

    MILLIS_TO_TS_MSK(ctms) AS created_ts_msk,
    MILLIS_TO_TS_MSK(utms) AS updated_ts_msk
FROM {{ source('mongo', 'b2b_product_product_certification_states_daily_snapshot') }}

{% endsnapshot %}
