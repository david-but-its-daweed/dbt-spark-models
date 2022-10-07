{% snapshot scd2_mongo_rfq_request %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='rfq_request_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}
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
    millis_to_ts_msk(utms) AS update_ts_msk,
    category_name
FROM {{ source('mongo', 'b2b_core_rfq_request_daily_snapshot') }} rfq
left join (
select distinct id, name as category_name
from
(
select id, name, row_number() over(partition by id order by load_ts desc) as rn 
    from {{ source('default', 'sat_category_info') }}
)
where rn = 1) cat on rfq.categories[1] = cat.id

{% endsnapshot %}
