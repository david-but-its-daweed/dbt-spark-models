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

with 
days as (


select 'Auto' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 2 as days
union all 
select 'Auto' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 7 as days
union all 
select 'Auto' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 7 as days
union all 
select 'Auto' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 5 as days
union all 
select 'Auto' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 1 as days
union all 
select 'Auto' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Auto' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 2 as days
union all 
select 'Auto' as linehaul_channel, 'delivering' as status, 3097 as status_int, 15 as days

union all 

select 'Aero' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 2 as days
union all 
select 'Aero' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 7 as days
union all 
select 'Aero' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 6 as days
union all 
select 'Aero' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 2 as days
union all 
select 'Aero' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 1 as days
union all 
select 'Aero' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Aero' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 1 as days
union all 
select 'Aero' as linehaul_channel, 'delivering' as status, 3097 as status_int, 5 as days

union all 
select 'Sea' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 5 as days
union all 
select 'Sea' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 5 as days
union all 
select 'Sea' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 10 as days
union all 
select 'Sea' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 21 as days
union all 
select 'Sea' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 3 as days
union all 
select 'Sea' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Sea' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 10 as days
union all 
select 'Sea' as linehaul_channel, 'delivering' as status, 3097 as status_int, 15 as days

union all 
select 'Rail' as linehaul_channel, 'pickupRequestSentToLogisticians' as status, 3010 as status_int, 2 as days
union all 
select 'Rail' as linehaul_channel, 'pickedUpByLogisticians' as status, 3020 as status_int, 7 as days
union all 
select 'Rail' as linehaul_channel, 'arrivedAtLogisticsWarehouse' as status, 3030 as status_int, 13 as days
union all 
select 'Rail' as linehaul_channel, 'departedFromLogisticsWarehouse' as status, 3040 as status_int, 23 as days
union all 
select 'Rail' as linehaul_channel, 'arrivedAtDestinations' as status, 3065 as status_int, 3 as days
union all 
select 'Rail' as linehaul_channel, 'customsDeclarationReleased' as status, 3090 as status_int, 1 as days
union all 
select 'Rail' as linehaul_channel, 'uploadToTemporaryWarehouse' as status, 3093 as status_int, 1 as days
union all 
select 'Rail' as linehaul_channel, 'delivering' as status, 3097 as status_int, 5 as days
),

days_manufacturing as (
select 'clientPayment' as status, 1000 as status_int, 4 as days
union all 
select 'advancePayment' as status, 1010 as status_int, 4 as days
union all 
select 'psi' as status, 2060 as status_int, 3 as days
union all
select 'fixingPsi', 2065 as status_int, 4 as days
union all 
select 'remainingPayment', 2070 as status_int, 4 as days
)

select 
    linehaul_channel, status, status_int, days
from days_manufacturing cross join (select distinct linehaul_channel from days)

union all

select * from days

order by 1, 3
