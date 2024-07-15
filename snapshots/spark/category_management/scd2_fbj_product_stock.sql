{% snapshot scd2_fbj_product_stock %}

{{
    config(
      meta = {
        'model_owner' : '@vasyukova_mn'
      },
      target_schema='category_management',
      unique_key='pId',
      strategy='timestamp',
      updated_at='uTm',
      file_format='delta'
    )
}}

    SELECT
        _id.pid AS pid,
        _id.wid AS wid,
        cid,
        ctm,
        reserved,
        stock,
        utm
    FROM {{ source('mongo', 'logistics_product_stocks_daily_snapshot') }}

{% endsnapshot %}
