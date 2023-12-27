{% snapshot scd2_mongo_merchant %}

{{
    config(
      target_schema='merchant',
      unique_key='merchant_id',
      strategy='timestamp',
      updated_at='updated_time',
      file_format='delta',
      invalidate_hard_deletes=True,
      meta = {
      'model_owner' : '@aleksandrov',
          'team' : 'merchant',
      }
    )
}}


SELECT
    merchant_id,
    MILLIS_TO_TS(created_time) AS created_time,
    MILLIS_TO_TS(updated_time) AS updated_time,
    MILLIS_TO_TS(activation_time) AS activation_time,
    MILLIS_TO_TS(disabled_time) AS disabled_time,
    name,
    origin,
    enabled,
    activated_by_merchant,
    disabling_reason,
    disabling_note,
    business_lines,
    category_ids
FROM {{ ref('merchant') }}

{% endsnapshot %}