{% snapshot scd2_mongo_сustomer_plans %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='user_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}



select      date(concat(cast(substr(tp, 0, 4) as int),'-',cast(substr(tp, 7, 7) as int)*3 - 2,'-01')) as quarter,
            egmv.amount/1000000 as plan,
            egmv.ccy as currency,
            uid as user_id,
            millis_to_ts_msk(utms)  AS update_ts_msk
from {{ source('mongo', 'b2b_core_customer_plans_daily_snapshot') }}

{% endsnapshot %}
