
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
else source end as souce
from {{ ref('key_amocrm_source') }}
)


select distinct
        AccountId as account_id,
        LossReasonId as loss_reason_id,
        companyId as company_id,
        companyName as company_name,
        coalesce(contactId, leadId) as contact_id,
        createdAt as created_at,
        max(type) over (partition by coalesce(contactId, leadId)) as type, 
        max(source) over (partition by coalesce(contactId, leadId)) as source, 
        max(campaign) over (partition by coalesce(contactId, leadId)) as campaign,
        leadId as lead_id,
        loss_reason_id,
        loss_reason,
        pipeline_name,
        pipelineId as pipeline_id,
        case when pipeline_id in ('7249567', '7314451', '7553579', '7120174', '7403522', '7120186') 
            then true else false end as validation,
        responsibleUser as admin_email,
        _id as owner_id,
        country,
        current_status,
        min(case when status in ('59912671', '60278571', '61650499', '59575366', '61529466', '59575418') then status_ts end) 
            over (partition by coalesce(contactId, leadId)) as created_ts_msk,
        min(case when status in ('59912675', '60278579', '61650503', '59575374', '61529470', '59575422') then status_ts end) 
            over (partition by coalesce(contactId, leadId)) as validated_ts_msk,
        max(funnel_status) over (partition by coalesce(contactId, leadId)) as funnel_status, 
        max(user_id) over (partition by coalesce(contactId, leadId)) as user_id
    from
    (
    select
        AccountId,
        LossReasonId,
        companyId,
        companyName,
        contactId,
        createdAt,
        campaign,
        source,
        type,
        leadId,
        lossReasons[0].id as loss_reason_id,
        lossReasons[0].name as loss_reason,
        pipeline_name,
        pipelineId,
        responsibleUser,
        _id,
        country,
        status as current_status,
        col.createdAt as status_ts,
        col.statuses[0]._id as status_id,
        col.statuses[0].name as status,
        funnel_status, user_id
    from
    (
    select 
        AccountId,
        LossReasonId,
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
        source.source as country,
        status,
        explode(statusChangedEvents),
        tags
    from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }} amo
    left join source on amo.source = source.id
    left join (select distinct funnel_status, user_id, amo_id from {{ ref('fact_customers') }}) on lead_id = amo_id
    )
    )
