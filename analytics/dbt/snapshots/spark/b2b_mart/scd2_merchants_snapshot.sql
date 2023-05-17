{% snapshot scd2_merchants_snapshot %}

   {{
       config(
         target_schema='b2b_mart',
         unique_key='_id',

         strategy='timestamp',
         updated_at='updatedTimeMs',
         file_format='delta',
         invalidate_hard_deletes=True,
       )
   }}


   SELECT *
   FROM {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }}
   {% endsnapshot %}
