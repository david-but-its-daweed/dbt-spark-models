{% snapshot scd2_interactions_snapshot %}

 {{
     config(
       target_schema='b2b_mart',
       unique_key='_id',

       strategy='timestamp',
       updated_at='update_ts_msk',
       file_format='delta',
       invalidate_hard_deletes=True,
     )
 }}


 SELECT *, millis_to_ts_msk(utms+2)  AS update_ts_msk
 FROM {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }}
 {% endsnapshot %}
