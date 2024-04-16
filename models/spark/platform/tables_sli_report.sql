{{ config(
    tags=['data_readiness'],
    meta = {
      'model_owner' : '@gburg'
    },
    schema='platform',
    materialized='view',
    'bigquery_load': 'true'
) }}

with data_readiness_aggregate (
    select source_id, input_name, input_type, date, min(ready_time_hours) ready_time_hours
    from platform.data_readiness
    where 
        date > NOW() - interval 3 months
        and date <= to_date(NOW())
        and input_rank = 1
    group by source_id, input_name, input_type, date
),
data (
           select source_id,
           business_name,
           date,
           alert_channels,
           target_sli,
           owner,
           description,
           max(ready_time_hours) as ready_time_hours,
           max(expected_time_utc_hours) as expected_time_utc_hours
    from data_readiness_aggregate
        left join platform_slo.slo_details on source_id = slo_details.slo_id
    where expected_time_utc_hours is not null
    group by source_id, business_name, target_sli, date, alert_channels, owner, description
    ),

data_3_month as (
    select source_id,
       business_name,
       target_sli,
       alert_channels,
       owner,
       description,
       max(expected_time_utc_hours) as expected_time_utc_hours,
       sum(
               case
                   when ready_time_hours > expected_time_utc_hours then 0
                   else 1
                   end)     as successes,
       count(distinct date) as days
    from data
    group by source_id, business_name, target_sli, alert_channels, owner, description
),

data_month as (
    select source_id,
       business_name,
       target_sli,
       alert_channels,
       owner,
       description,
       sum(
               case
                   when ready_time_hours > expected_time_utc_hours then 0
                   else 1
                   end)     as successes,
       count(distinct date) as days
    from data
    where date > NOW() - interval 1 months
    group by source_id, business_name, target_sli, alert_channels, owner, description
),

data_week as (
    select source_id,
       business_name,
       target_sli,
       alert_channels,
       owner,
       description,
       sum(
               case
                   when ready_time_hours > expected_time_utc_hours then 0
                   else 1
                   end)     as successes,
       count(distinct date) as days
    from data
    where date > NOW() - interval 1 weeks
    group by source_id, business_name, target_sli, alert_channels, owner, description
)

select source_id,
    data_3_month.business_name,
    priority,
    data_week.successes / data_week.days * 100 as sli_last_week,
    data_month.successes / data_month.days * 100 as sli_last_month,
    data_3_month.successes / data_3_month.days * 100 as sli__3_months,
    data_3_month.target_sli,
    data_3_month.expected_time_utc_hours,
    data_3_month.alert_channels,
    data_3_month.owner,
    data_3_month.description
from data_3_month
    left join data_month using (source_id)
    left join data_week using (source_id)
    left join platform_slo.slo_details on source_id = slo_details.slo_id
order by source_id
