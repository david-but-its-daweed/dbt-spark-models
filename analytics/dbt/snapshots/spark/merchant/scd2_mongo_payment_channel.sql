{% snapshot scd2_mongo_merchant_payment_channel %}

{{
    config(
      target_schema='merchant',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_time',
      file_format='delta'
    )
}}

SELECT  _id as id,
        errorReasons as error_reasons,
        merchantId as merchant_id,
        methodId as method_id,
        status,
        type,
        updatedTime as updated_time
FROM {{ source('mongo', 'core_merchant_payment_channels_daily_snapshot') }}

{% endsnapshot %}
