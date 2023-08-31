{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


select  interaction_id, 
user_id,
source, 
type,
campaign,
website_form,
interaction_type,
incorrect_attribution,
incorrect_utm,
key as utm_label,
value as utm_value
from
(
select  interaction_id, 
user_id,
source, 
type,
campaign,
website_form,
interaction_type,
incorrect_attribution,
incorrect_utm,
explode(utm_labels) 
from
(
select 
_id as interaction_id, 
uid as user_id,
source as source, 
type as type,
campaign as campaign,
websiteForm as website_form,
interactionType as interaction_type,
incorrectAttribution as incorrect_attribution,
incorrectUtm as incorrect_utm,
map_from_entries(utmLabels) as utm_labels
from b2b_mart.scd2_interactions_snapshot
)
)
