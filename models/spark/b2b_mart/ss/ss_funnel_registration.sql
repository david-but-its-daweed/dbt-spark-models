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


WITH main AS (
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
    cast(min(case when type = 'selfServiceRegistrationFinished' then event_ts_msk end) as Date) as selfServiceRegistrationFinished_Date,
    
    
    max(case when type = 'registrationOpen' and payload.step = '0.0' then 1 else 0 end) AS first_step_start,
    min(case when type = 'registrationOpen' and payload.step = '0.0' then event_ts_msk end) AS first_step_start_ts,
    cast(min(case when type = 'registrationOpen' and payload.step = '0.0' then event_ts_msk end) AS Date) AS first_step_start_date,

    max(case when type = 'registrationSubmitStep' and payload.step = '0.0' then 1 else 0 end) AS first_step_end,
    min(case when type = 'registrationSubmitStep' and payload.step = '0.0' then event_ts_msk end) AS first_step_end_ts,
    cast(min(case when type = 'registrationSubmitStep' and payload.step = '0.0' then event_ts_msk end) AS Date) AS first_step_end_date,

    max(case when type = 'registrationOpen' and payload.step = '1.0' then 1 else 0 end) AS second_step_start,
    min(case when type = 'registrationOpen' and payload.step = '1.0'  then event_ts_msk end) AS second_step_start_ts,
    cast(min(case when type = 'registrationOpen' and payload.step = '1.0' then event_ts_msk end) AS Date) AS second_step_start_date,

    MAX(
        CASE 
            WHEN type = 'registrationSubmitStep'
             AND payload.step = (
                     CASE
                         WHEN DATE(event_ts_msk) >= '2024-11-18' THEN '1.0'
                         ELSE '3.0'
                     END
             )
            THEN 1
            ELSE 0
        END
    ) AS second_step_end,
    MIN(
        CASE 
            WHEN type = 'registrationSubmitStep'
             AND payload.step = (
                     CASE
                         WHEN DATE(event_ts_msk) >= '2024-11-18' THEN '1.0'
                         ELSE '3.0'
                     END
             )
            THEN event_ts_msk
        END
    ) AS second_step_end_ts,
    DATE(MIN(
        CASE 
            WHEN type = 'registrationSubmitStep'
             AND payload.step = (
                     CASE
                         WHEN DATE(event_ts_msk) >= '2024-11-18' THEN '1.0'
                         ELSE '3.0'
                     END
             )
            THEN event_ts_msk
        END
    )) AS second_step_end_date
from  {{ source('b2b_mart', 'device_events') }}
where partition_date >= '2024-04-01'
and type in (
    'signIn',
    'registrationOpen',
    'registrationSubmitStep',
    'selfServiceRegistrationFinished')
group by 1
)


SELECT 
    user_id,
    signIn,
    signIn_ts,
    signIn_Date,
    UserInfo_start,
    UserInfo_start_ts,
    UserInfo_start_Date,
    UserInfo_end,
    UserInfo_end_ts,
    UserInfo_end_Date,
    CompanyInfo_start,
    CompanyInfo_start_ts,
    CompanyInfo_start_Date,
    CompanyInfo_end,
    CompanyInfo_end_ts,
    CompanyInfo_end_Date,
    ImportInfo_start,
    ImportInfo_start_ts,
    ImportInfo_start_Date,
    ImportInfo_end,
    ImportInfo_end_ts,
    ImportInfo_end_Date,
    cnpgInfo_start,
    cnpgInfo_start_ts,
    cnpgInfo_start_Date,
    cnpgInfo_end,
    cnpgInfo_end_ts,
    cnpgInfo_end_Date,
    selfServiceRegistrationFinished,
    selfServiceRegistrationFinished_ts,
    selfServiceRegistrationFinished_Date,
    greatest(selfServiceRegistrationFinished, second_step_end, second_step_start, first_step_end, first_step_start) AS first_step_start,
    first_step_start_ts,
    first_step_start_date,
    greatest(selfServiceRegistrationFinished, second_step_end, second_step_start, first_step_end) AS first_step_end,
    first_step_end_ts,
    first_step_end_date,
    greatest(selfServiceRegistrationFinished, second_step_end, second_step_start) AS second_step_start,
    second_step_start_ts,
    second_step_start_date,
    greatest(selfServiceRegistrationFinished, second_step_end) AS second_step_end,
    second_step_end_ts,
    second_step_end_date
FROM main
