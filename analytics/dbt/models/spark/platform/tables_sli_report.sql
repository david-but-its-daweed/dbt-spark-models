{{ config(
    schema='platform',
    materialized='view',
) }}

with data (
    select source_id, 
        business_name,
        date,
        alert_channels, 
        target_sli,
        owner, 
        description,
        max(ready_time_hours) as ready_time_hours, 
        max(expected_time_utc_hours) as expected_time_utc_hours
    from platform.data_readiness
        left join platform_slo.slo_details on source_id = slo_details.slo_id
    where 1=1
        and expected_time_utc_hours is not null
        and date > NOW() - interval 2 months
        and date <= to_date(NOW())
        and input_rank = 1
    group by source_id, business_name, target_sli, date, alert_channels, owner, description
),

data2 as (
    select *,
        case 
            when ready_time_hours > expected_time_utc_hours then 0
            else 1
        end as is_success
    from data
    order by date desc
)

select source_id,
    business_name,
    sum(is_success) / count(1) * 100 as sli,
    target_sli,
    max(expected_time_utc_hours) as expected_time_utc_hours,
    alert_channels,
    owner,
    description
from data2
group by source_id, business_name, target_sli, alert_channels, owner, description
order by source_id