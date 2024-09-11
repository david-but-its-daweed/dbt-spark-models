{% snapshot scd2_mongo_variant_committed_fulfillment %}

{{
    config(
        meta = {
          'team' : 'merchant',
          'model_owner' : '@slava'
        },
      target_schema='merchant',
      unique_key='variant_id',
      strategy='check',
      check_cols=['cft'],
      file_format='delta'
    )
}}

    SELECT
        product_id,
        variant_id,
        cft
    FROM {{ ref('variant_committed_fulfillment') }}

{% endsnapshot %}
