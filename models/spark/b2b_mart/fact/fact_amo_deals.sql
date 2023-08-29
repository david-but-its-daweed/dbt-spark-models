{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}



with users as (
select
distinct user_id, b.leadId 
from {{ ref('fact_customers') }}
join (
    select distinct contactId, leadId from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
) a on amo_id = a.leadId
join (
    select distinct contactId, leadId from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
) b on a.contactId = b.contactId
where amo_id is not null)


select distinct 
        user_id,
        amo.leadId as amo_id,
        lossReasons[0].name as sub_status_amo,
        case when lossReasons[0].name = "Слишком дорого - карго" then "Price too high compared to cargo"
            when lossReasons[0].name = "Слишком дорого - в белую" then "Price too high compared to official delivery"
            when lossReasons[0].name = "Не устроили условия" then "Client doesnt like our contract"
            when lossReasons[0].name = "Перестали выходить на связь" then "Client no response"
            when lossReasons[0].name = "Не нашли товар" and pipeline_name = 'SDR' then "Product not found"
            when lossReasons[0].name = "Не можем привезти товар - санкции" then "Sanctioned product"
            when lossReasons[0].name = "Не можем привезти - таможня" then "Impossible to deliver"
            when status_name in ("закрыто и не реализовано", "решение отложено") then "Other" end as sub_status,
        status_name as status_amo,
        case when status_name in ("закрыто и не реализовано", "решение отложено") then "Cancelled"
        else "Converting" end as status,
        millis_to_ts_msk(createdAt*1000) as created_at
        from
        {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }} amo
        left join {{ ref('key_amo_status') }} st on amo.status = st.status_id
        left join users on amo.leadId = users.leadId
        where users.user_id is not null
