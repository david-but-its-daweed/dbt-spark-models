{% snapshot scd2_merchant_appendixes_snapshot %}

 {{
     config(
       target_schema='b2b_mart',
       unique_key='_id',

       file_format='delta'
       strategy='check',
       check_cols='all',
     )
 }}


 SELECT *
 FROM {{ source('mongo', 'b2b_core_merchant_appendixes_daily_snapshot') }}
 {% endsnapshot %}
