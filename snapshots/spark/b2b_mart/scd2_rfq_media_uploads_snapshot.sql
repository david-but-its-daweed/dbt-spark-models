{% snapshot scd2_rfq_media_uploads_snapshot %}

 {{
     config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
       target_schema='b2b_mart',
       unique_key='_id',

       file_format='delta',
       strategy='check',
       check_cols=['createdTimeMs'],
       invalidate_hard_deletes=True,
     )
 }}


 SELECT *, imageId||rfqId||createdTimeMs as _id
 FROM {{ source('mongo', 'b2b_core_rfq_media_uploads_daily_snapshot') }}
 {% endsnapshot %}
