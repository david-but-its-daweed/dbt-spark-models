{% snapshot scd2_mongo_variant_appendixes %}

    {{
        config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
          target_schema='b2b_mart',
          strategy='check',
          unique_key='variant_id',
          file_format='delta',
          check_cols=['minimum_box_qnty', 'medium_box_qnty','maximum_box_qnty','minimum_box_hight','medium_box_hight','maximum_box_hight','minimum_box_width','medium_box_width','maximum_box_width','minimum_box_length','medium_box_length','maximum_box_length','price_min_qnty','price_medium_qnty','price_max_qnty','article','trademark'],
        )
    }}
select _id as variant_id,
    pId as product_id,
    bs.q[0] as minimum_box_qnty,
    bs.q[1] as medium_box_qnty,
    bs.q[2] as maximum_box_qnty,
    bs.h[0] as minimum_box_hight,
    bs.h[1] as medium_box_hight,
    bs.h[2] as maximum_box_hight,
    bs.w[0] as minimum_box_width,
    bs.w[1] as medium_box_width,
    bs.w[2] as maximum_box_width,
    bs.l[0] as minimum_box_length,
    bs.l[1] as medium_box_length,
    bs.l[2] as maximum_box_length,
   prc[0] AS price_min_qnty,
   prc[1] AS price_medium_qnty,
   prc[2] AS price_max_qnty,
   article,
   trMrk as trademark
from  {{ source('mongo', 'b2b_product_variant_appendixes_daily_snapshot') }}


{% endsnapshot %}
