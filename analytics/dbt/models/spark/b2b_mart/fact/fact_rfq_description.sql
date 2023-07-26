{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with orders as (
select 
    rfq_request_id,
    category_1,
    category_2,
    category_3,
    greatest(greatest(size(images), size(variant.images)), 0) as images,
    name,
    plnk as link,
    coalesce(variant.price.amount, price.amount) as price,
    coalesce(variant.price.ccy, price.ccy) as currency,
    coalesce(variant.quantity, qty) as quantity,
    variant_number,
    variants,
    manufacturing_days,
    coalesce(descr, variant.description) as description,
    variant.color,
    variant.size,
    oid as order_id,
    '' as customer_request_id,
    friendlyId as friendly_id,
    '' as model,
    null as questionnaire,
    millis_to_ts_msk(ctms) as created_time
from 
(
select _id as rfq_request_id,
categories[0] as cat1,
categories[1] as cat2,
categories[2] as cat3,
images,
name,
friendlyId,
plnk,
price,
qty,
oid, 
descr,
productVariants[variant_number] as variant,
variant_number + 1 as variant_number,
size(productVariants) as variants,
manufacturingDays as manufacturing_days,
ctms
from {{ ref('scd2_rfq_request_snapshot') }}
cross join (
    select explode(variant_number) as variant_number
    from
    (
        select sequence(0, max(size(productVariants)) - 1) as variant_number
        from {{ ref('scd2_rfq_request_snapshot') }}
    )
    )
order by ctms, rfq_request_id
)
left join (
select category_id, name as category_1
    from {{ source('mart', 'category_levels') }}
) cat1 on cat1 = cat1.category_id
left join (
select category_id, name as category_2
    from {{ source('mart', 'category_levels') }}
) cat2 on cat2 = cat2.category_id
left join (
select category_id, name as category_3
    from {{ source('mart', 'category_levels') }}
) cat3 on cat3 = cat3.category_id
where  greatest(size(images), size(variant.images)) > 0
),

deals as (
select 
    rfq_request_id,
    category_1,
    category_2,
    category_3,
    greatest(greatest(size(images), size(variant.images)), 0) as images,
    name,
    plnk as link,
    coalesce(variant.price.amount, price.amount) as price,
    coalesce(variant.price.ccy, price.ccy) as currency,
    coalesce(variant.quantity, qty) as quantity,
    variant_number,
    variants,
    manufacturing_days,
    coalesce(descr, variant.description) as description,
    variant.color,
    variant.size,
    '' as order_id,
    crid as customer_request_id,
    friendlyId as friendly_id,
    model,
    questionnaire,
    millis_to_ts_msk(ctms) as created_time
from 
(
select _id as rfq_request_id,
categories[0] as cat1,
categories[1] as cat2,
categories[2] as cat3,
images,
name,
friendlyId,
plnk,
price,
qty,
crid, 
descr,
productVariants[variant_number] as variant,
variant_number + 1 as variant_number,
size(productVariants) as variants,
manufacturingDays as manufacturing_days,
ctms,
model,
questionnaire
from {{ ref('scd2_customer_rfq_request_snapshot') }}
cross join (
    select explode(variant_number) as variant_number
    from
    (
        select sequence(0, max(size(productVariants)) - 1) as variant_number
        from {{ ref('scd2_customer_rfq_request_snapshot') }}
    )
    )
where (variant_number = 0 or size(productVariants) >= variant_number + 1)
order by ctms, rfq_request_id
)
left join (
select category_id, name as category_1
    from {{ source('mart', 'category_levels') }}
) cat1 on cat1 = cat1.category_id
left join (
select category_id, name as category_2
    from {{ source('mart', 'category_levels') }}
) cat2 on cat2 = cat2.category_id
left join (
select category_id, name as category_3
    from {{ source('mart', 'category_levels') }}
) cat3 on cat3 = cat3.category_id
)



select 
    rfq_request_id,
    category_1,
    category_2,
    category_3,
    images,
    name,
    link,
    price,
    currency,
    quantity,
    variant_number,
    variants,
    manufacturing_days,
    description,
    color,
    size,
    order_id,
    customer_request_id,
    friendly_id,
    model,
    
    questionnaire.additionalInfo.annualVolume as annual_volume,
    questionnaire.additionalInfo.assembly as assembly,
    questionnaire.additionalInfo.availability.productionDays as production_days,
    questionnaire.additionalInfo.availability.type as availability,
    questionnaire.additionalInfo.considersSimilar as considers_similar,
    questionnaire.additionalInfo.endUseOfProduct.type as end_use,
    
    questionnaire.customisation.instruction as instruction,
    questionnaire.customisation.instructionTyped.type as instruction_typed,
    questionnaire.customisation.logo as logo,
    questionnaire.customisation.logoTyped.type as logo_typed,
    questionnaire.customisation.packingDesign.type as packing_design,
    questionnaire.customisation.packingType.type as packing_type,
    created_time
from
(
select * from orders
union all
select * from deals
)
