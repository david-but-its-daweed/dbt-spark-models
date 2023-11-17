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


with t_categories as (
select 
key.m as merchant_id,
max(split(replace(replace(replace(value, "[", ""), "]", ""), '"', ""), ",")) as categories
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'Speciality'
group by key.m
), 

t_partner_merchant as (
select max(value = 'true') as partner_merchant, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'PartnerMerchant'
group by key.m
),

t_business_essence as (
select max(split(replace(replace(replace(replace(value, "[", ""), "]", ""), '"', ""), " ", ""), ",")) as business_essence,
        key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'BusinessEssence'
group by key.m
),

t_rfq_participant as (
select max(value = 'true') as rfq_participant, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'RFQParticipant'
group by key.m
),

t_passed_verification as (
select max(value = 'true') as passed_verification, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'PassedVerification'
group by key.m
),

t_export_activities as (
select max(value) as export_activities, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'ExportActivities'
group by key.m
),


t_top_rfq as (
select max(value = 'true') as top_rfq, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'TopRFQ'
group by key.m
),

t_comment as (
select max(value) as comment, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'Comment'
group by key.m
)

select
    coalesce(t_categories.merchant_id, t_partner_merchant.merchant_id,
            t_business_essence.merchant_id, t_rfq_participant.merchant_id, t_passed_verification.merchant_id,
            t_export_activities.merchant_id, t_top_rfq.merchant_id, t_comment.merchant_id) as merchant_id,
    t_categories.categories,
    t_partner_merchant.partner_merchant,
    t_business_essence.business_essence,
    t_rfq_participant.rfq_participant,
    t_passed_verification.passed_verification,
    t_export_activities.export_activities,
    t_top_rfq.top_rfq,
    t_comment.comment
from t_categories
full join t_partner_merchant using (merchant_id)
full join t_business_essence using (merchant_id)
full join t_rfq_participant using (merchant_id)
full join t_passed_verification using (merchant_id)
full join t_export_activities using (merchant_id)
full join t_top_rfq using (merchant_id)
full join t_comment using (merchant_id)
