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

with categories as (
select distinct
key.m as merchant_id,
split(replace(replace(replace(value, "[", ""), "]", ""), '"', ""), ",") as categories
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'Speciality'
), 

partner_merchant as (
select value = 'true' as partner_merchant, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'PartnerMerchant'
),

business_essence as (
select split(replace(replace(replace(replace(value, "[", ""), "]", ""), '"', ""), " ", ""), ",") as business_essence,
        key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'BusinessEssence'
),

rfq_participant as (
select value = 'true' as rfq_participant, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'RFQParticipant'
),

passed_verification as (
select value = 'true' as passed_verification, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'PassedVerification'
),

export_activities as (
select value export_activities, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'ExportActivities'
),


top_rfq as (
select value = 'true' as top_rfq, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'TopRFQ'
),

comment as (
select value export_activities, key.m as merchant_id
from {{ ref('scd2_merchant_attributes_snapshot') }}
where key.n = 'Comment'
)

select * from categories
full join partner_merchant using (merchant_id)
full join business_essence using (merchant_id)
full join rfq_participant using (merchant_id)
full join passed_verification using (merchant_id)
full join export_activities using (merchant_id)
full join top_rfq using (merchant_id)
full join comment using (merchant_id)

