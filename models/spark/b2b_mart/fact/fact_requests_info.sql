{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


select distinct
_id as customer_request_id,
questionnaire.additionalInfo.assembly,
questionnaire.additionalInfo.availability.productionDays as production_days,
questionnaire.additionalInfo.availability.type as production_type,
questionnaire.additionalInfo.considersSimilar as consider_similar,
questionnaire.additionalInfo.endUseOfProduct.type as usage_type,
questionnaire.customisation.instruction,
questionnaire.customisation.instructionTyped.type as instuction_type,
questionnaire.customisation.logo,
questionnaire.customisation.logoTyped.type as logo_type,
questionnaire.customisation.packingDesign.type as packing_design,
questionnaire.customisation.packingType.type as packing_type
from {{ ref('scd2_customer_requests_snapshot') }}
where dbt_valid_to is null
