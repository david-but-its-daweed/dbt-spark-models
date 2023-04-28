{{ config(
    schema='platform',
    materialized='view'
) }}

with accesses as (
    select
      date,
      bucket,
      usage_type,
      prefix,
      prefix_size as path_length,
      usage_category,
      sum(accesses) as items,
      '1000s of requests' as item_name,
      sum(accesses / 1000 * price) as cost
    FROM platform.s3_accesses_usage
        left join platform.s3_operation_usage_types using (operation)
        left join platform.aws_pricing using (usage_type)
    group by date,
        bucket,
        usage_type,
        usage_category,
        prefix,
        prefix_size),

    files as (
      select
          date,
          bucket,
          usage_type,
          prefix,
          path_length,
          usage_category,
          sum (size) / 1024 / 1024 / 1024 / 30 as items,
          'Gb' as item_name,
          sum (size / 1024 / 1024 / 1024 / 30 * price / (1 + coalesce (coef, 0))) as cost
      FROM platform.s3_file_usage
          left join platform.storage_class_to_usage_type using (storage_class)
          left join platform.s3_price_coeffs using (bucket, usage_type)
          left join platform.aws_pricing using (usage_type)
      group by date,
          bucket,
          usage_type,
          usage_category,
          prefix,
          path_length)
SELECT *
from (select *
      from accesses
      UNION ALL
      select *
      from files)
