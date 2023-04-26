{% snapshot scd2_mongo_interactions %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='interaction_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}

with utm as (
select _id as interaction_id,
max(map_from_entries(utmLabels)["utm_campaign"]) as utm_campaign,
max(map_from_entries(utmLabels)["utm_source"]) as utm_source,
max(map_from_entries(utmLabels)["utm_medium"]) as utm_medium,
max(map_from_entries(utmLabels)["utm_content"]) as utm_content,
max(map_from_entries(utmLabels)["utm_term"]) as utm_term
from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }}
group by 1
)

select i._id as interaction_id,
description,
did as device_id,
uid as user_id,
type,
source,
campaign,
websiteForm as website_form,
utm_campaign,
utm_source,
utm_medium,
utm_content,
utm_term,
friendlyId as interaction_friendly_id,
popupRequestID as popup_request_id,
promocodeId as promocode_id,
millis_to_ts_msk(ctms) AS created_ts_msk,
millis_to_ts_msk(utms) AS update_ts_msk

from {{ source('mongo', 'b2b_core_interactions_daily_snapshot') }} i left join utm on i._id = utm.interaction_id

{% endsnapshot %}
