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

select
    interaction_id,
    interaction_created_at,
    type,
    source,
    campaign,
    amo_id,
    months_between(next_interaction_ts, interaction_created_at) >= 6 OR next_interaction_ts IS NULL AS incorrect_attribution,
    next_interaction_ts is null as last_interaction_type,
    rn = 1 as first_interaction_type
from
(
select
interaction_id,
interaction_created_at,
type,
source,
campaign,
amo_id,
lead(interaction_created_at) over (partition by amo_id order by interaction_created_at) as next_interaction_ts,
row_number() over (partition by amo_id order by interaction_created_at) as rn
from
(
select distinct
col._id as interaction_id,
millis_to_ts_msk(col.createdAt) as interaction_created_at,
col.interaction.type as type,
col.interaction.source as source,
col.interaction.campaign as campaign,
leadId as amo_id
from
(
select explode(interactionChangedEvents), leadId, interaction as current_interaction
from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
)
)
)
