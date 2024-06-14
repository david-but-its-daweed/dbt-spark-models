{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "signIn_Date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

select 

    user['userId'] as user_id,
    max(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE then 1 else 0 end) as signIn,
    min(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE then event_ts_msk end) as signIn_ts,
    cast(min(case when type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE then event_ts_msk end) as Date) as signIn_Date,

    max(case when type = 'registrationOpen' and payload.step = '0.0' then 1 else 0 end) UserInfo_start,
    min(case when type = 'registrationOpen' and payload.step = '0.0' then event_ts_msk end) as UserInfo_start_ts,
    cast(min(case when type = 'registrationOpen' and payload.step = '0.0' then event_ts_msk end) as Date) as UserInfo_start_Date,

    max(case when type = 'registrationSubmitStep' and payload.step = '0.0' then 1 else 0 end) UserInfo_end,
    min(case when type = 'registrationSubmitStep' and payload.step = '0.0' then event_ts_msk end) as UserInfo_end_ts,
    cast(min(case when type = 'registrationSubmitStep' and payload.step = '0.0' then event_ts_msk end) as Date) as UserInfo_end_Date,

    max(case when type = 'registrationOpen' and payload.step = '1.0' then 1 else 0 end) CompanyInfo_start,
    min(case when type = 'registrationOpen' and payload.step = '1.0'  then event_ts_msk end) as CompanyInfo_start_ts,
    cast(min(case when type = 'registrationOpen' and payload.step = '1.0' then event_ts_msk end) as Date) as CompanyInfo_start_Date, 

    max(case when type = 'registrationSubmitStep' and payload.step = '1.0' then 1 else 0 end) CompanyInfo_end,
    min(case when  type = 'registrationSubmitStep' and payload.step = '1.0' then event_ts_msk end) as CompanyInfo_end_ts,
    cast(min(case when type = 'registrationSubmitStep' and payload.step = '1.0' then event_ts_msk end) as Date) as CompanyInfo_end_Date,

    max(case when type = 'registrationOpen' and payload.step = '2.0' then 1 else 0 end) ImportInfo_start,
    min(case when  type = 'registrationOpen' and payload.step = '2.0' then event_ts_msk end) as ImportInfo_start_ts,
    cast(min(case when type = 'registrationOpen' and payload.step = '2.0' then event_ts_msk end) as Date) as ImportInfo_start_Date,

    max(case when type = 'registrationSubmitStep' and payload.step = '2.0' then 1 else 0 end) ImportInfo_end,
    min(case when type = 'registrationSubmitStep' and payload.step = '2.0' then event_ts_msk end) as ImportInfo_end_ts,
    cast(min(case when type = 'registrationSubmitStep' and payload.step = '2.0' then event_ts_msk end) as Date) as ImportInfo_end_Date,

    max(case when type = 'registrationOpen' and payload.step = '3.0' then 1 else 0 end) cnpgInfo_start,
    min(case when type = 'registrationOpen' and payload.step = '3.0' then event_ts_msk end) as cnpgInfo_start_ts,
    cast(min(case when type = 'registrationOpen' and payload.step = '3.0' then event_ts_msk end) as Date) as cnpgInfo_start_Date,

    max(case when type = 'registrationSubmitStep' and payload.step = '3.0' then 1 else 0 end) cnpgInfo_end,
    min(case when type = 'registrationSubmitStep' and payload.step = '3.0' then event_ts_msk end) as cnpgInfo_end_ts,
    cast(min(case when type = 'registrationSubmitStep' and payload.step = '3.0' then event_ts_msk end) as Date) as cnpgInfo_end_Date,

    max(case when type = 'selfServiceRegistrationFinished' then 1 else 0 end)  selfServiceRegistrationFinished,
    min(case when type = 'selfServiceRegistrationFinished' then event_ts_msk end) as selfServiceRegistrationFinished_ts,
    cast(min(case when type = 'selfServiceRegistrationFinished' then event_ts_msk end) as Date) as selfServiceRegistrationFinished_Date
    
from  {{ source('b2b_mart', 'device_events') }}
where partition_date >= '2024-04-01'
and type in (
    'signIn',
    'registrationOpen',
    'registrationSubmitStep',
    'selfServiceRegistrationFinished')
group by 1
