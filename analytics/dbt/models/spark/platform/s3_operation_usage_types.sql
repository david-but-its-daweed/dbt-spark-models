-- We don't have consistent way to map operation to request tier, so we rely on adhoc rules based on
-- https://docs.aws.amazon.com/AmazonS3/latest/userguide/aws-usage-report-understand.html 
-- and https://support.console.aws.amazon.com/support/home?region=eu-central-1#/case/?displayId=12544738091&language=en

{{ config(
    schema='platform',
    materialized='table'
) }}

select distinct
    operation,
    CASE
        WHEN operation like 'REST.COPY.OBJECT_GET' then 'undefined'
        WHEN operation like '%_GET' then 'undefined'
        WHEN operation like 'REST.POST.MULTI_OBJECT_DELETE' then 'undefined'
        WHEN operation like 'REST.GET.UPLOADS' then 'EUC1-Requests-Tier1'
        WHEN operation like 'REST.PUT.%' then 'EUC1-Requests-Tier1'
        WHEN operation like 'REST.COPY.%' then 'EUC1-Requests-Tier1'
        WHEN operation like 'REST.POST.%' then 'EUC1-Requests-Tier1'
        WHEN operation like 'REST.LIST.%' then 'EUC1-Requests-Tier1'
        WHEN operation like 'REST.GET.BUCKET' then 'EUC1-Requests-Tier1'
        WHEN operation like 'REST.GET.%' then 'EUC1-Requests-Tier2'
        WHEN operation like 'REST.HEAD.OBJECT' then 'EUC1-Requests-Tier2'
        ELSE 'undefined'
        end as usage_type
FROM platform.s3_accesses_usage