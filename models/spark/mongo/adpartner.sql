{{ config(
    schema='mongo',
    materialized='view',
) }}
select name,
       email,
       transform(trackers, x -> named_struct( 'id', x._id, 'kind', x.kind)) as trackers
from {{ source('mongo', 'events_ad_partners_daily_snapshot') }}