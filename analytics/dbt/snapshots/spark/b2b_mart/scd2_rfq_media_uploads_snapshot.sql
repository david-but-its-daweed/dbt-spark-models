{% snapshot scd2_rfq_media_uploads_snapshot %}

 {{
     config(
       target_schema='b2b_mart',
       unique_key='imageId',

       file_format='delta',
       strategy='check',
       check_cols='all',
       invalidate_hard_deletes=True,
     )
 }}


 SELECT *
 FROM {{ source('mongo', 'b2b_core_rfq_media_uploads_daily_snapshot') }}
 {% endsnapshot %}
