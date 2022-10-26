{% snapshot scd2_mongo_product_fast_delivery_info_daily_snapshot %} 

{{
    config(
      target_schema='category_management',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_time',
      file_format='delta'
    )
}}

SELECT
    concat_ws('-', productid, country, subdivision) AS id,
    productid AS product_id,
    country,
    subdivision,
    to_timestamp('{{ var("end_date_ymd") }}') AS updated_time
FROM {{ source('mongo', 'product_fast_delivery_info_daily_snapshot') }}

{% endsnapshot %}
