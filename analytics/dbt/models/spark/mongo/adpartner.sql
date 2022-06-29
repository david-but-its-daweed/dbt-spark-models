{{ config(
    schema='mongo',
    materialized='view',
) }}
select name,
       email,
       struct(
               trackers._id as id,
               trackers.kind
           ) as trackers
from {{ source('mongo', 'events_ad_partners_daily_snapshot') }}