{% snapshot scd2_order_gross_profit_final_estimated %}

{{
    config(
      target_schema='ads_models',
      unique_key='order_id',
      strategy='check',
      check_cols='all',
      file_format='delta',
      invalidate_hard_deletes=True,
      meta = {
      'model_owner' : '@zhabrev',
          'team' : 'advertising',
      }
    )
}}

    SELECT
        order_id,
        order_date_msk,
        order_gross_profit_final,
        order_gross_profit_final_estimated
    FROM {{ ref('gold_orders') }}
    DISTRIBUTE BY ABS(HASH(order_id)) % 10

{% endsnapshot %}
