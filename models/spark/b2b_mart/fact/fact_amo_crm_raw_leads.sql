
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

with source as (
select cast(id as int) as id, 
case when source = 'russia' then 'RU'
when source = 'brazil' then 'BR'
when source = 'analytics' then 'Analytics'
else source end as source
from {{ ref('key_amocrm_source') }}),

statuses as (
select leadId,
explode(statusChangedEvents) as st
from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
),

phone as (
    select lead_id, min(phone) as phone
    from {{ ref('fact_amo_crm_contacts_phones') }}
    group by lead_id
)



select distinct
    pipeline_id,
    validation,
    pipeline_name,
    lead_id,
    phone,
    company_id,
    company_name,
    contact_id,
    created_at,
    created_ts_msk,
    validated_ts_msk,
    case when validated_ts_msk is not null then 'Validated'
        when current_status_id = 143 then 'Rejected'
        else 'In Progress' end as validation_status,
    loss_reason,
    type,
    source,
    campaign,
    admin_email,
    owner_id,
    country,
    current_status_id,
    current_status,
    current_status_ts,
    funnel_status,
    user_id
from
(
select distinct
        pipelineId as pipeline_id,
        case when pipelineId in (
            '7249567',
            '7314451',
            '7553579',
            '7120174',
            '7403522',
            '7120186'
        ) then true else false end as validation,
        pipeline_name,
        leadId as lead_id,
        phone.phone,
        companyId as company_id,
        companyName as company_name,
        coalesce(contactId, leadId) as contact_id,
        millis_to_ts_msk(createdAt) as created_at,
        millis_to_ts_msk(
            min(
                case when 
                pipelineId = '7314451'
                or (pipelineId = '7249567' and status in ('IN PROGRESS', 'In progress'))
                or (pipelineId = '7553579' and status = 'In progress')
                or pipelineId = '7120174'
                or (pipelineId = '7403522' and status = 'Взят в работу')
                or (pipelineId = '7120186' and status in ('Взят в работу', 'Взяли в работу'))
                then coalesce(status_ts, createdAt) end
            ) over (partition by coalesce(contactId, leadId))
        ) as created_ts_msk,

        millis_to_ts_msk(
            min(
            case when 
                (pipelineId = '7314451' and status in ('Closed - won'))
                or (pipelineId = '7249567' and status in ('Closed - won'))
                or (pipelineId = '7120174' and status in ('Квалифицирован', 'Лид квалифицирован', 'Уточняем запрос клиента'))
                or (pipelineId = '7403522' and status in ('Квалифицирован', 'Лид квалифицирован'))
                or (pipelineId = '7120186' and status in ('Квалифицирован', 'Лид квалифицирован', 'Предложение отправлено'))
            then status_ts end) 
            over (partition by coalesce(contactId, leadId))
        ) as validated_ts_msk,
        loss_reason,
    
        max(type) over (partition by coalesce(contactId, leadId)) as type, 
        max(source) over (partition by coalesce(contactId, leadId)) as source, 
        max(campaign) over (partition by coalesce(contactId, leadId)) as campaign,
        responsibleUser as admin_email,
        _id as owner_id,
        country,
        current_status as current_status_id,
        max(case when status_id = current_status then status end) over (partition by coalesce(contactId, leadId)) as current_status,
        millis_to_ts_msk(max(case when status_id = current_status then status_ts end) over (partition by coalesce(contactId, leadId))) as current_status_ts,
        max(funnel_status) over (partition by coalesce(contactId, leadId)) as funnel_status, 
        max(user_id) over (partition by coalesce(contactId, leadId)) as user_id
    from
    (
    select
        AccountId,
        loss_reason,
        companyId,
        companyName,
        contactId,
        createdAt,
        campaign,
        source,
        type,
        leadId,
        pipeline_name,
        pipelineId,
        responsibleUser,
        _id,
        country,
        status as current_status,
        st.createdAt as status_ts,
        st.statuses[0]._id as status_id,
        st.statuses[0].name as status,
        funnel_status, user_id
    from
    (
    select distinct
        AccountId,
        rejectReason as loss_reason,
        companyId,
        companyName,
        contactId,
        createdAt,
        interaction.campaign as campaign,
        interaction.source as source,
        interaction.type as type,
        leadId,
        lossReasons,
        pipeline.name as pipeline_name,
        pipelineId,
        responsibleUser.name as responsibleUser,
        responsibleUser._id,
        s.source as country,
        status,
        tags, funnel_status, user_id
    from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }} amo
    left join source s on amo.source = s.id
    left join (select distinct funnel_status, user_id, amo_id from {{ ref('fact_customers') }}) on leadId = amo_id
    ) left join statuses using (leadId)
    )
    left join phone on leadId = lead_id
)
