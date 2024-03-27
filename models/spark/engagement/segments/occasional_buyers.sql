{{ config(
    schema='engagement',
    materialized='table',
    meta = {
      'model_owner' : '@evstvia',
      'team': 'clan',
      'priority_weight': '150',

      'segment_upload': 'true',
      'segment_type': 'device',
      'segment_author_email': 'evstvia@joom.com',
      'segment_name': 'occ_buyers',
      'segment_description': 'Occasional buyers',
      'segment_ttl_days': 28,
      'segment_upload_cassandra': 'true',
      'segment_upload_spark': 'true'
    }
) }}


with users as (
    select
        real_user_id
    from {{ ref('user_segments') }}
    where
        user_segment = 'Occasional buyers'
        and effective_ts <= date'{{ var("start_date_ymd") }}'
        and next_effective_ts > date'{{ var("start_date_ymd") }}'
)

select distinct
    d.device_id
from users as u
join mart.link_device_real_user as d using (real_user_id)
