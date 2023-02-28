{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}

with deals as (select 
dealId , 
dealType, 
date(FROM_UNIXTIME(estimatedEndDate/1000)) as estimated_date,
estimatedGmv.* ,
interactionId,
moderatorId,
name,
date(FROM_UNIXTIME(updatedTime/1000)) as updated_date,
userId,
row_number() over (partition by dealId order by updatedTime desc) as rn
from
(
select payload.* 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'dealChanged'
{% if is_incremental() %}
       and date(FROM_UNIXTIME(updatedTime/1000)) <= date'{{ var("start_date_ymd") }}'
     {% else %}
       and partition_date  <= date'2023-02-27'
     {% endif %}
)
)

select 
dealId as deal_id, 
dealType as deal_type, 
estimated_date,
amount as estimated_gmv,
interactionId as interaction_id,
moderatorId as moderator_id,
name as deal_name,
updated_date,
userId as user_id,
date('{{ var("start_date_ymd") }}') as partition_date_msk
from deals 
where rn = 1
