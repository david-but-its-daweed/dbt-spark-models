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



with 
deals as (

select distinct
all_leads.lead_id as deal_id,
contact_id,
tochka.lead_id as amo_id
from {{ ref('fact_amo_crm_contacts_phones') }} all_leads
join
(
select max(lead_id) as lead_id, contact_id
from
(select 
min(contact_id) as contact_id, lead_id
from {{ ref('fact_amo_crm_contacts_phones') }}
where lead_id in (
select distinct amo_id from {{ ref('fact_amo_attribution_interaction') }} i
LEFT JOIN {{ ref('fact_amo_crm_raw_leads') }} AS amo ON i.amo_id = amo.lead_id
where i.source = 'tochka' and validation = true
)
group by lead_id
)
group by contact_id
) tochka using (contact_id)
where tochka.lead_id != all_leads.lead_id
and contact_id != 24268427
and all_leads.lead_id not in (
  select distinct lead_id from {{ ref('fact_amo_crm_raw_leads') }}
  where pipeline_name in ('Квалификация Rocket', 'Специальные предложения Rocket')
  )
),


statuses as (
select distinct col._id as status_id, col.name as status_name
from
(
select explode(pipeline.statuses) from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
where pipeline.name = 'Продажи Rocket'
)
),

status_history as (
    select
        leadId, 
        max(case when status in ('Заявка на расчет', 'Предварительный расчет') then created_at end) as request_retrieval,
        max(case when status in ('Уточнение информации у Клиента') then created_at end) as info_clarification,
        max(case when status in ('Подготовка КП', 'Запрос на поиск', 'RFQ запрос отправлен') then created_at end) as find_retrieval,
        max(case when status in ('КП отправлено/Ожидание ОС', 'Ожидание обратной связи') then created_at end) as pricing_sent,
        max(case when status in ('Переговоры') then created_at end) as negotiation,
        max(case when status in ('Оформление сделки/Ожидание оплаты', 'Оформление сделки/Подписание и ожидание оплаты') then created_at end) as signing_and_payment,
        max(case when status in ('Производство и доставка') then created_at end) as manufacturing
    from
    (select 
        leadId,
        col.statuses.name[0] as status, 
        millis_to_ts_msk(col.createdAt) as created_at
        from
    (
        select 
        leadId,
        explode(statusChangedEvents)
        from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
        where pipeline.name = 'Продажи Rocket'
    )
    )
    group by leadId
    
),

deal_statuses as (
select leadId as deal_id, rejectReason as loss_reason, status_name, 
        request_retrieval, info_clarification, find_retrieval, pricing_sent,
        negotiation, signing_and_payment, manufacturing
from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
left join statuses on status = status_id
left join status_history using (leadId)
),

notes as (select
amo_id,
max(case when rn = 5 then text end) as note_1,
max(case when rn = 4 then text end) as note_2,
max(case when rn = 3 then text end) as note_3,
max(case when rn = 2 then text end) as note_4,
max(case when rn = 1 then text end) as note_5
from
(
select leadId as amo_id, text, row_number() over (partition by leadId order by rn desc) as rn
from
(
select leadId, col.*, row_number() over (partition by leadId order by 1) as rn
from 
(
select leadId, explode(notes) from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
where notes is not null
)
)
)
group by amo_id),

tasks as (
    select
        leadId as amo_id,
        millis_to_ts_msk(createdAt) as task_created_at,
        entityType as task_type,
        text
    from
    (
    select
        leadId, col.*, row_number() over (partition by leadId order by col.createdAt desc) as rn
    from
    (
    select leadId, explode(tasks) from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
    where tasks is not null
    )
    )
    where rn = 1
)

select 
  amo.amo_id, 
  amo.phone,
  amo.created_at as user_created_time, 
  CASE WHEN deal_statuses.deal_id IS NOT NULL THEN 'Validated'
    ELSE amo.validation_status END AS validation_status,
  coalesce(amo.validated_date, request_retrieval) as validated_date,
  CASE WHEN deal_statuses.deal_id IS NOT NULL THEN null
    ELSE amo.reject_reason END AS reject_reason,
  campaign,
  deal_statuses.deal_id, loss_reason, status_name, 
  request_retrieval, info_clarification, find_retrieval, pricing_sent,
  negotiation, signing_and_payment, manufacturing,
  coalesce(notes1.note_1, notes.note_1) as note_1,
  coalesce(notes1.note_2, notes.note_2) as note_2,
  coalesce(notes1.note_3, notes.note_3) as note_3,
  coalesce(notes1.note_4, notes.note_4) as note_4,
  coalesce(notes1.note_5, notes.note_5) as note_5,
  coalesce(tasks1.task_created_at, tasks.task_created_at) as task_created_at,
  coalesce(tasks1.task_type, tasks.task_type) as task_type,
  coalesce(tasks1.text, tasks.text) as text
from
(
select
amo_id, 
phone,
amo.created_at, 
CASE
        WHEN amo.validated_ts_msk IS NOT NULL THEN 'Validated'
        WHEN amo.created_ts_msk IS NOT NULL AND amo.current_status NOT IN (
            'Закрыто и не реализовано', 'Решение отложено', 'Closed - lost', 'Closed - won'
        ) THEN 'InProgress'
        WHEN amo.created_ts_msk IS NOT NULL AND amo.current_status IN (
            'Закрыто и не реализовано', 'Решение отложено', 'Closed - lost', 'Closed - won'
        ) THEN 'Rejected'
    END AS validation_status,
CASE WHEN amo.validated_ts_msk IS NOT NULL THEN DATE(amo.validated_ts_msk) END AS validated_date,
CASE
        WHEN amo.validated_ts_msk IS NULL AND CAST(amo.current_status_id AS string) NOT IN (
            '59912675', '60278579', '61650503', '59575374', '61529470', '59575422', '59388859', '60278575'
        ) THEN amo.loss_reason
END AS reject_reason,
i.campaign,
row_number() over (partition by amo_id order by 1) as rn
from {{ ref('fact_amo_attribution_interaction') }} i
left join {{ ref('fact_amo_crm_raw_leads') }} AS amo ON i.amo_id = amo.lead_id
left join (
  select lead_id as amo_id, max(phone) as phone from {{ ref('fact_amo_crm_contacts_phones') }} group by 1
  ) phone using (amo_id)
where i.source = 'tochka' and validation
) amo
left join deals on amo.amo_id = deals.amo_id
left join deal_statuses on deals.deal_id = deal_statuses.deal_id
left join notes on notes.amo_id = amo.amo_id
left join notes notes1 on notes1.amo_id = deals.deal_id
left join tasks on tasks.amo_id = amo.amo_id
left join tasks tasks1 on tasks1.amo_id = deals.deal_id
where rn = 1
