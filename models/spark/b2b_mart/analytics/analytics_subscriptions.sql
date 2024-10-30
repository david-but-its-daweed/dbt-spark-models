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
        payment_id,
        user_id,
        add_months(created_time, (time_payed * package_duration)) as payment_created_time,
        add_months(created_date, (time_payed * package_duration)) as payment_created_date,
        package_id,
        package_duration_unit,
        package_duration,
        package_price,
        package_price_ccy,
        created_time,
        created_date,
        price,
        currency,
        promocode_id,
        promocode,
        status,
        subscribtion_months,
        time_payed
from
(
select
        payment_id,
        user_id,
        payment_created_time,
        package_id,
        package_duration_unit,
        package_duration,
        package_price,
        package_price_ccy,
        created_time,
        created_date,
        price,
        currency,
        promocode_id,
        promocode,
        status,
        subscribtion_months,
        posexplode(
            array_repeat(
                    price,
                    cast(ceil(subscribtion_months)/package_duration as int)
                )
            ) as (time_payed, price_2)
from
(
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
        millis_to_ts(createdTimeMs) as created_time,
        to_date(millis_to_ts(createdTimeMs)) as created_date,
        price.amount/1000000 as price,
        price.ccy as currency,
        promocodeSnapshot._id as promocode_id,
        promocodeSnapshot.code as promocode,
        status,
        ceil(
            case when status = "active"
                then months_between(
                
                    greatest(to_timestamp(current_date()),
                    coalesce(millis_to_ts(cancellationTime),
                    millis_to_ts(nextChargeAttemptTime)
                    )), millis_to_ts(createdTimeMs)
                
            )
            else
            months_between(
                coalesce(
                    millis_to_ts(cancellationTime),
                    millis_to_ts(nextChargeAttemptTime)
                    ), millis_to_ts(createdTimeMs)
                )
            end
            ) 
            as subscribtion_months
from {{ source('mongo', 'b2b_core_analytics_subscriptions_daily_snapshot') }}
)
)
