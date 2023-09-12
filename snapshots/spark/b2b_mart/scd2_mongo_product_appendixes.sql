{% snapshot scd2_mongo_product_appendixes.sql %}

    {{
        config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
          target_schema='b2b_mart',
          strategy='check',
          unique_key='product_id',
          file_format='delta',
          check_cols=['brand', 'quantity_first','quantity_second','quantity_third','is_top_product','supplier_link','supplier_product_link'],
        )
    }}

     select _id as product_id,
                  brand,
                  prcQtyDis[0] AS quantity_first,
                  prcQtyDis[1] AS quantity_second,
                  prcQtyDis[2] AS quantity_third,
                  searchTags.topProduct as is_top_product,
                  supplierLink as supplier_link,
                  supplierProductLink as supplier_product_link
           from   {{ source('mongo', 'b2b_core_product_appendixes_daily_snapshot') }}

{% endsnapshot %}
