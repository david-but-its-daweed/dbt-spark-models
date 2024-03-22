
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
),

amo_admins as (
        select *
        from {{ ref('key_amo_admin') }}
    ),
    
sales as (
        select
        contactId,
        case when users_admin_role not in ('Support', 'SDR') then users_admin_role
            else max(case when max_sales_time = 1 then task_admin_role end) end as sales_role,
        case when users_admin_role not in ('Support', 'SDR') then users_admin_name
            else max(case when max_sales_time = 1 then task_admin_name end) end as sales_name,
        case when users_admin_role not in ('Support', 'SDR') then users_admin_email
            else max(case when max_sales_time = 1 then task_admin_email end) end as sales_email,
        case when users_admin_role not in ('Support', 'SDR') then users_admin_id
            else max(case when max_sales_time = 1 then task_admin_id end) end as sales_id
            
    from
    (
    select
        contactId,
        leadId,
        createdAt,
        users.admin_id as users_admin_id,
        users.admin_role as users_admin_role,
        users.admin_name as users_admin_name,
        users.admin_email as users_admin_email,
        task.admin_id as task_admin_id,
        task.admin_role as task_admin_role,
        task.admin_name as task_admin_name,
        task.admin_email as task_admin_email,
        case when task.admin_role not in ('Support', 'SDR')
            then row_number() over (partition by contactId, task.admin_role in ('Support', 'SDR') order by createdAt desc) as max_sales_time
    from
    (
    select 
        contactId,
        leadId,
        createdAt,
        responsibleUser.*,
        explode(tasks)
    from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
    )
    left join amo_admins task on col.responsibleUserId = task.admin_id
    left join amo_admins users on _id = users.admin_id
    )
    group by contactId, users_admin_role, users_admin_name, users_admin_email, users_admin_id
    ),
    
    support as (
    select
        leadId,
        case when users_admin_role in ('Support', 'SDR') then users_admin_role
            else max(case when max_support_time = 1 then task_admin_role end) end as support_role,
        case when users_admin_role in ('Support', 'SDR') then users_admin_name
            else max(case when max_support_time = 1 then task_admin_name end) end as support_name,
        case when users_admin_role in ('Support', 'SDR') then users_admin_email
            else max(case when max_support_time = 1 then task_admin_email end) end as support_email,
        case when users_admin_role in ('Support', 'SDR') then users_admin_id
            else max(case when max_support_time = 1 then task_admin_id end) end as support_id,
            
        sales_role,
        sales_name,
        sales_email,
        sales_id
            
    from
    (
    select
        contactId,
        leadId,
        createdAt,
        users.admin_id as users_admin_id,
        users.admin_role as users_admin_role,
        users.admin_name as users_admin_name,
        users.admin_email as users_admin_email,
        task.admin_id as task_admin_id,
        task.admin_role as task_admin_role,
        task.admin_name as task_admin_name,
        task.admin_email as task_admin_email,
        case when task.admin_role in ('Support', 'SDR')
            then row_number() over (partition by contactId, task.admin_role in ('Support', 'SDR') order by createdAt desc) as max_support_time
    from
    (
    select 
        contactId,
        leadId,
        createdAt,
        responsibleUser.*,
        explode(tasks)
    from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
    )
    left join amo_admins task on col.responsibleUserId = task.admin_id
    left join amo_admins users on _id = users.admin_id
    )
    left join sales using (contactId)
    group by
        leadId,
        users_admin_role,
        users_admin_name,
        users_admin_email,
        users_admin_id,
        sales_role,
        sales_name,
        sales_email,
        sales_id
    )



select distinct
    pipeline_id,
    validation,
    pipeline_name,
    lead_id,
    case when validation then lead_id else validation_lead_id end as validation_lead_id,
    phone,
    company_id,
    company_name,
    contact_id,
    created_at,
    created_ts_msk,
    legal_entity,
    CASE WHEN country = 'BR' and legal_entity THEN 'Validated' 
        WHEN country = 'BR' and not legal_entity THEN 'Rejected'
        WHEN country = 'BR' then 'In Progress' END as marketing_validation_status,
    validated_ts_msk,
    case when validated_ts_msk is not null then 'Validated'
        when current_status_id = 143 then 'Rejected'
        else 'In Progress' end as validation_status,
    amo_request_ts_msk,
    case when amo_request_ts_msk is not null then 'Amo Request'
        else 'No Request' end as amo_request_status,
    loss_reason,
    type,
    source,
    campaign,


    
    case when owner_email is not null then '' else admin_name end as admin_name,
    coalesce(owner_email, admin_email) as admin_email,
    coalesce(owner_id, admin_id) as owner_id,
    coalesce(owner_role, admin_role) as admin_role,
    
    country,
    current_status_id,
    current_status,
    current_status_ts,
    funnel_status,
    user_id,
    
    support_role,
    support_name,
    support_email,
    support_id,


    case when owner_email is not null then '' else sales_name end as sales_name,
    coalesce(owner_email, sales_email) as sales_email,
    coalesce(owner_id, sales_id) as sales_id,
    coalesce(owner_role, sales_role) as sales_role
from
(
select distinct
        pipelineId as pipeline_id,
        validation,
        pipeline_name,
        hasLegalEntity as legal_entity,
        leadId as lead_id,
        phone.phone,
        companyId as company_id,
        companyName as company_name,
        coalesce(contactId, leadId) as contact_id,
        min(case when validation then leadId end) over (partition by coalesce(contactId, leadId)) as validation_lead_id,
        millis_to_ts_msk(createdAt) as created_at,
        millis_to_ts_msk(
            case when pipelineId in ('7314451', '7249567', '7553579', '7120174') then createdAt
            when pipelineId in ('7403522', '7120186') then 
                min(
                case when 
                   (pipelineId = '7403522' and status = 'Взят в работу')
                    or (pipelineId = '7120186' and status in ('Взят в работу', 'Взяли в работу'))
                then coalesce(status_ts, createdAt) end
            ) over (partition by coalesce(leadId))
            end
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
            over (partition by coalesce(leadId))
        ) as validated_ts_msk,

        millis_to_ts_msk(
            min(
            case when status in ('Заявка на расчет')
            or (status = 'Закрыто и не реализовано' and loss_reason in ('Передали Sales', 'Передали КАМам'))
            then status_ts end) 
            over (partition by coalesce(leadId))
        ) as amo_request_ts_msk,
    
        loss_reason,
    
        type, 
        source, 
        campaign,
        admin_name, admin_email, admin_id, admin_role,

        owner_id, owner_email, owner_role,
    
        country,
        current_status as current_status_id,
        max(case when status_id = current_status then status end) over (partition by coalesce(contactId, leadId)) as current_status,
        millis_to_ts_msk(max(case when status_id = current_status then status_ts end) over (partition by coalesce(contactId, leadId))) as current_status_ts,
        max(funnel_status) over (partition by coalesce(contactId, leadId)) as funnel_status, 
        max(user_id) over (partition by coalesce(contactId, leadId)) as user_id
    from
    (
    select
        case when pipelineId in (
            '7249567',
            '7314451',
            '7553579',
            '7120174',
            '7403522',
            '7120186'
        ) then true else false end as validation,
        AccountId,
        hasLegalEntity,
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
        admin_name, admin_email, admin_id, admin_role,

        owner_id, owner_email, owner_role,
    
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
        hasLegalEntity,
        companies[0]._id as companyId,
        companies[0].name as companyName,
        contacts[0]._id as contactId,
        createdAt,
        interaction.campaign as campaign,
        interaction.source as source,
        interaction.type as type,
        leadId,
        lossReasons,
        pipeline.name as pipeline_name,
        pipelineId,
        admin_name, admin_email, admin_id, admin_role,
        
        owner_id, owner_email, owner_role,

        s.source as country,
        status,
        tags, funnel_status, user_id
    from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }} amo
    left join source s on amo.source = s.id
    left join (
        select distinct
            funnel_status, user_id, amo_id, 
            owner_id, owner_email, owner_role
        from {{ ref('fact_customers') }}
    ) on leadId = amo_id
    left join amo_admins on admin_id = responsibleUser._id
    ) left join statuses using (leadId)
    )
    left join phone on leadId = lead_id
)
left join support on leadId = lead_id
