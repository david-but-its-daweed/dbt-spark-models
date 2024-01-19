{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

select 'noOperationsStarted' as status, 10 as status_int
union all
select 'advancePaymentRequested' as status, 20 as status_int
union all
select 'advancePaymentInProgress' as status, 25 as status_int
union all
select 'advancePaymentAcquired' as status, 27 as status_int
union all
select 'manufacturingAndQCInProgress' as status, 30 as status_int
union all
select 'remainingPaymentRequested' as status, 40 as status_int
union all
select 'remainingPaymentInProgress' as status, 45 as status_int
union all
select 'remainingPaymentAcquired' as status, 50 as status_int
union all
select 'completePaymentRequested' as status, 60 as status_int
union all
select 'completePaymentInProgress' as status, 65 as status_int
union all
select 'completePaymentAcquired' as status, 70 as status_int
union all
select 'merchantAcquiredPayment' as status, 80 as status_int
