{{ config(
    meta = {
      'model_owner' : '@leonid.enov'
    },
    schema='mongo',
    materialized='view',
) }}
select
    _id.partner as partner_id,
    to_date(millis_to_ts(_id.t + (24 * 60 * 60 * 1000))) as dim_date_id,
    spent as spend
from {{ source('mongo', 'events_ad_spent_internal_daily_snapshot') }}