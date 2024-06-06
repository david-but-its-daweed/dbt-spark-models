{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}

with accesses as (select date,
                         bucket,
                         db,
                         table_name,
                         usage_type,
                         prefix,
                         path_length,
                         usage_category,
                         sum(accesses)                as items,
                         '1000s of requests'          as item_name,
                         sum(accesses / 1000 * price) as cost
                  FROM {{ ref("s3_file_access_v2") }}
                      left join {{ ref("aws_pricing") }} using (usage_type)
                  group by date,
                      bucket,
                      db,
                      table_name,
                      usage_type,
                      usage_category,
                      prefix,
                      path_length),

     files as (select date,
                      bucket,
                      db,
                      table_name,
                      usage_type,
                      prefix,
                      path_length,
                      usage_category,
                      sum(size) / 1024 / 1024 / 1024                                        as items,
                      'Gb'                                                                  as item_name,
                      sum(size / 1024 / 1024 / 1024 / 30 * price / (1 + coalesce(coef, 0))) as cost
               FROM {{ ref("s3_file_usage_v2") }}
                   left join {{ ref("storage_class_to_usage_type") }} using (storage_class)
                   left join {{ ref("s3_price_coeffs") }} using (bucket, usage_type)
                   left join {{ ref("aws_pricing") }} using (usage_type)
               group by date,
                   bucket,
                   db,
                   table_name,
                   usage_type,
                   usage_category,
                   prefix,
                   path_length)
SELECT case
           when db = 'ads' or bucket = 'joom-analytics-ads' then 'ads'
           when db = 'adtech' or bucket = 'joom-analytics-adtech' then 'adtech'
           when db = 'recom' or bucket = 'joom-analytics-recom' then 'recom'
           when db = 'logistics' or bucket = 'joom-analytics-logistics' then 'logistics'
           when db = 'search' or bucket = 'joom-analytics-search' then 'search'
           when db = 'push' or bucket = 'joom-analytics-push' then 'push'
           when db = 'platform' or bucket in (
                                              'joom-analytics-cache',
                                              'joom-analytics-cache',
                                              'joom-analytics-experiments',
                                              'joom-analytics-users'
               ) then 'platform'
           else 'unknown'
           end as team,
       *
from (select *
      from accesses
      UNION ALL
      select *
      from files)
