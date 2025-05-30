{% snapshot scd2_product_appendixes_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='_id',

      file_format='delta',
      strategy='check',
      check_cols=['brand', 'cstmzbl', 'isFocused', 'reducedDuty', 'supplierLink', 'supplierProductLink', 'trMrk'],
      invalidate_hard_deletes=True,
    )
}}


SELECT *
FROM {{ source('mongo', 'b2b_product_product_appendixes_daily_snapshot') }}
{% endsnapshot %}
