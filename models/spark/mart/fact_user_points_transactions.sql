{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'date_msk',
      'bigquery_upload_horizon_days': '14',
    }
) }}

SELECT _id                                                                                    as id,
       userId                                                                                 as user_id,
       kind,
       type,
       cast((effectiveUSD / 1000000) as decimal(38, 6))                                       as effective_usd,
       hidden,
       refId                                                                                  as refid,
       to_date(cast(cast(conv(substring(_id, 0, 8), 16, 10) as bigint) + 10800 as timestamp)) as date_msk,
       amount,
       badOrderId                                                                             as bad_order_id,
       index,
       pending
FROM {{ source('mongo', 'user_points_transactions_daily_snapshot') }}
where to_date(cast(cast(conv(substring(_id, 0, 8), 16, 10) as bigint) + 10800 as timestamp)) < to_date(NOW())

