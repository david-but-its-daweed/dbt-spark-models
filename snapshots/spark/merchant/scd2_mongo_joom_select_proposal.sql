{% snapshot scd2_mongo_joom_select_proposal %}

{{
    config(
    meta = {
      'model_owner' : '@n.rykov'
    },
      target_schema='merchant',
      unique_key='proposal_id',
      strategy='timestamp',
      updated_at='updated_time',
      file_format='delta'
    )
}}

    SELECT
        proposal_id,
        created_time,
        updated_time,
        product_id,
        merchant_id,
        status_history,
        target_variant_prices,
        cancel_info
    FROM {{ ref('joom_select_proposal') }}

{% endsnapshot %}
