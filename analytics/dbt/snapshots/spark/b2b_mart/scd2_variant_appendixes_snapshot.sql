{% snapshot scd2_variant_appendixes_snapshot %}

 {{
     config(
       target_schema='b2b_mart',
       unique_key='_id',

       file_format='delta',
       strategy='check',
       check_cols='all',
       invalidate_hard_deletes=True,
     )
 }}


 SELECT *
 FROM {{ source('mongo', 'b2b_core_variant_appendixes_daily_snapshot') }}
 {% endsnapshot %}
