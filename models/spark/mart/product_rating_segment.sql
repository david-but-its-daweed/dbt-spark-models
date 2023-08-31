{{ config(
    schema='mart',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    file_format='parquet',
    meta = {
      'team': 'categories',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_upload_horizon_days': '1'
    }
) }}

with category_price_range_ratings as
    (select
            --- we look at 3 levels categories except cases where it is kids clothing, then we look at 5
            coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.id,level_4_category.id),level_3_category.id),level_2_category.id, level_1_category.id) as level_3_category_id,
            coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.name,level_4_category.name),level_3_category.name),level_2_category.name, level_1_category.name) as level_3_category_name,


            if(gmv_initial/product_quantity <= 5, '1) 0-5',
                if(gmv_initial/product_quantity <= 10, '2) 5-10',
                    if(gmv_initial/product_quantity <= 20, '3) 10-20', '4) 20+'))) as price_range,



            avg((rating_counts.count_1_star + rating_counts.count_2_star*2 + rating_counts.count_3_star*3 + rating_counts.count_4_star*4 + rating_counts.count_5_star*5)/(rating_counts.count_1_star + rating_counts.count_2_star + rating_counts.count_3_star + rating_counts.count_4_star + rating_counts.count_5_star)) as strata_rating,
            count(*) as orders

        from
            {{ source('mart', 'star_order_2020') }}
            join {{ source('mart', 'category_levels') }} using(category_id)
        where
            partition_date >= '{{ var("start_date_ymd") }}' - interval 83 days
            and partition_date <= '{{ var("start_date_ymd") }}'
            and approval_time_utc is not null
            and (customer_refund_reason not in (1,13) or customer_refund_reason is null)

        group by 1,2,3),


    product_price_ranges as
         (select
            product_id,
            sum(gmv_initial)/sum(product_quantity) as avg_price
        from
            {{ source('mart', 'star_order_2020') }}
            join {{ source('mart', 'category_levels') }} using(category_id)
        where
            partition_date >= '{{ var("start_date_ymd") }}'  - interval 364 days
            and partition_date <= '{{ var("start_date_ymd") }}'
            and approval_time_utc is not null
            and (customer_refund_reason not in (1,13) or customer_refund_reason is null)

        group by 1),


    product_ratings as
    (select
        product_id,
        (rating_counts.count_1_star + rating_counts.count_2_star*2 + rating_counts.count_3_star*3 + rating_counts.count_4_star*4 + rating_counts.count_5_star*5)/(rating_counts.count_1_star + rating_counts.count_2_star + rating_counts.count_3_star + rating_counts.count_4_star + rating_counts.count_5_star) as product_rating,
        rating_counts.count_1_star + rating_counts.count_2_star + rating_counts.count_3_star + rating_counts.count_4_star + rating_counts.count_5_star as number_or_ratings

    from
        {{ source('mart', 'product_rating_counters') }}
    where
        next_effective_ts > '{{ var("start_date_ymd") }}'
        and effective_ts <= '{{ var("start_date_ymd") }}' ),


        product_category_price_range_rating as

        (select
            product_id,
            product_rating,
            number_or_ratings,
            coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.id,level_4_category.id),level_3_category.id),level_2_category.id, level_1_category.id) as level_3_category_id,
            coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.name,level_4_category.name),level_3_category.name),level_2_category.name, level_1_category.name) as level_3_category_name,
            if(avg_price <= 5, '1) 0-5',
                if(avg_price <= 10, '2) 5-10',
                    if(avg_price <= 20, '3) 10-20', '4) 20+'))) as price_range

        from
            product_ratings
            join
                (select
                    product_id,
                    category_id

                from  {{ source('mart', 'dim_published_product_min') }}
                where next_effective_ts > '{{ var("start_date_ymd") }}'
                and effective_ts <= '{{ var("start_date_ymd") }}' )  using(product_id)
            join product_price_ranges using(product_id)
            join {{ source('mart', 'category_levels') }} using(category_id))

select
    to_date('{{ var("start_date_ymd") }}') as partition_date,
    product_id,
    level_3_category_id,
    level_3_category_name,
    price_range,
    product_rating,
    if(product_rating - least(strata_rating, 4.6) > 0.2, 'good rating',
        if(product_rating - least(strata_rating, 4.6) < -0.2, 'bad rating', 'normal rating')) as rating_segment,
    strata_rating,
    number_or_ratings as product_number_of_ratings

from
    product_category_price_range_rating
    join category_price_range_ratings using(level_3_category_id,level_3_category_name, price_range)
