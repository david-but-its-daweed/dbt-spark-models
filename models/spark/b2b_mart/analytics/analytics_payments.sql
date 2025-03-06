{{ config(
    schema='joompro_analytics_internal_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false',
      'priority_weight': '150'
    }
) }}


select
        _id as payment_id,
        usedId as user_id,
        millis_to_ts(createdTimeMs) as payment_created_time,
        packageSnapshot._id as package_id,
        packageSnapshot.duration.unit as package_duration_unit,
        case when packageSnapshot.duration.unit = 'year' then packageSnapshot.duration.value*12
            else packageSnapshot.duration.value end as package_duration,
        packageSnapshot.price.amount/1000000 as package_price,
        packageSnapshot.price.ccy as package_price_ccy,
        millis_to_ts(paidTimeMs) as paid_time,
        to_date(millis_to_ts(paidTimeMs)) as paid_date,
        price.amount/1000000 as price,
        price.ccy as currency,
        promocodeSnapshot._id as promocode_id,
        promocodeSnapshot.code as promocode,
        coalesce(promocodeSnapshot.discount.fixed.amount, 0) as discount_fixed,
        coalesce(promocodeSnapshot.discount.percentage.percentage, 0) as discount_percentage,
        rate,
        status
from {{ source('mongo', 'b2b_core_analytics_payments_daily_snapshot') }}
join (select
            1000000/rate as rate,
            explode(sequence(effective_date, least(next_effective_date - interval 1 day, current_date()), interval 1 day)) AS date
         from mart.dim_currency_rate
where currency_code = "BRL"
    ) ON to_date(millis_to_ts(createdTimeMs)) = date
