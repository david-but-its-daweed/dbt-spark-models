{{ config(
    schema='mart',
    materialized='incremental',
    partition_by=['partition_date'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'priority_weight': '150',
      'model_owner' : '@leonid.enov',
      'team': 'categories',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date'
    }
) }}

with orders_with_feedback as
        (select
            product_id,
            created_time_utc,
            partition_date,
            coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.id,level_4_category.id),level_3_category.id),level_2_category.id, level_1_category.id) as level_3_category_id,
            coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.name,level_4_category.name),level_3_category.name),level_2_category.name, level_1_category.name) as level_3_category_name,


            if(gmv_initial/product_quantity <= 5, '1) 0-5',
                if(gmv_initial/product_quantity <= 10, '2) 5-10',
                    if(gmv_initial/product_quantity <= 20, '3) 10-20', '4) 20+'))) as price_range,

            if(refund_reason = 'quality' and datediff(refund_time_utc,created_time_utc) <= 80,1,0) as if_quality_refund,
            if((review_stars in (1,2) or sizeFitId in (1,3)) and datediff(review_time_utc,created_time_utc) <= 80,1,0) as bad_rating,
            coalesce(tracking_delivered_time_utc, delivered_time_utc) as delivered_time,
            gmv_initial,
            product_quantity,

            if(array_contains(negative_feedback_reason_answer_ids, 'inaccurateDescription'), 1, 0) as inaccurate_description,
            if(array_contains(negative_feedback_reason_answer_ids, 'damage'), 1, 0) as damage,
            if(array_contains(negative_feedback_reason_answer_ids, 'wrongColorOrSizeOrItemWasSent'), 1, 0) as wrongs,
            if(array_contains(negative_feedback_reason_answer_ids, 'quality'), 1, 0) as quality,
            if(array_contains(negative_feedback_reason_answer_ids, 'incompletePackage'), 1, 0) as incomplete_package,
            if(SizeFitID = 1, 1, 0) as too_small_size,
            if(SizeFitID = 3, 1, 0) as too_large_size,
            if(customer_refund_reason = 4, 1,0) as bad_quality_refund,
            if(customer_refund_reason = 23, 1,0) as not_as_descibe_refund,
            if(customer_refund_reason not in (4,23) and refund_reason = 'quality', 1, 0) as other_quality_refund

        from {{ source('mart', 'star_order_2020') }}
        left join {{ source('mart', 'category_levels') }} using(category_id)
        left join {{ source('mart', 'core_review_quiz_answers_scd2') }} as reviews
            on _id = order_id
            and reviews.next_effective_ts > partition_date + interval 80 days
            and partition_date + interval 80 days >= reviews.effective_ts
        left join
            (select order_id,
                user_delivered_time_utc,
                tracking_delivered_time_utc
            from logistics_mart.fact_order) using(order_id)


        where
            (tracking_delivered_time_utc is not null
            or delivered_time_utc is not null)
            and not_delivered_time_utc is null
            and (refund_reason = 'quality' or refund_reason is null)
        order by created_time_utc),

        strats_metric as
            (select
                level_3_category_id,
                level_3_category_name,
                price_range,
                count(*) as orders,
                sum(gmv_initial) as gmv_initial,
                sum(gmv_initial*if(if_quality_refund = 1 or bad_rating = 1,1,0))/sum(gmv_initial) as strata_NF_share_delivered,

                sum(gmv_initial*bad_rating)/sum(gmv_initial) as strata_bad_rating,
                sum(gmv_initial*if_quality_refund)/sum(gmv_initial) as strata_quality_refunds,


                sum(gmv_initial*inaccurate_description)/sum(gmv_initial) as strata_inaccurate_description,
                sum(gmv_initial*damage)/sum(gmv_initial) as strata_damage,
                sum(gmv_initial*wrongs)/sum(gmv_initial) as strata_wrongs,
                sum(gmv_initial*quality)/sum(gmv_initial) as strata_quality,
                sum(gmv_initial*incomplete_package)/sum(gmv_initial) as strata_incomplete_package,
                sum(gmv_initial*too_small_size)/sum(gmv_initial) as strata_too_small_size,
                sum(gmv_initial*too_large_size)/sum(gmv_initial) as strata_too_large_size,


                sum(gmv_initial*bad_quality_refund)/sum(gmv_initial) as strata_bad_quality_refund,
                sum(gmv_initial*not_as_descibe_refund)/sum(gmv_initial) as strata_not_as_descibe_refund,
                sum(gmv_initial*other_quality_refund)/sum(gmv_initial) as strata_other_quality_refund

            from
                orders_with_feedback
            where
                datediff(delivered_time,created_time_utc) <= 80
                and partition_date between '{{ var("start_date_ymd") }}'  - interval 524 day and '{{ var("start_date_ymd") }}'  - interval 160 day
            group by 1,2,3
            order by 1,2,3),


         product_metric as
            (select
                product_id,
                coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.id,level_4_category.id),level_3_category.id),level_2_category.id, level_1_category.id) as level_3_category_id,
                coalesce(if(level_2_category.id = '1475519040525567064-3-2-118-4293140116',coalesce(level_5_category.name,level_4_category.name),level_3_category.name),level_2_category.name, level_1_category.name) as level_3_category_name,


                if(sum(gmv_initial)/sum(product_quantity) <= 5, '1) 0-5',
                    if(sum(gmv_initial)/sum(product_quantity) <= 10, '2) 5-10',
                        if(sum(gmv_initial)/sum(product_quantity) <= 20, '3) 10-20', '4) 20+'))) as price_range,

                count(*) as product_orders,
                sum(if(if_quality_refund = 1 or bad_rating = 1,1,0))/sum(1) as product_NF_share_delivered,

                sum(gmv_initial*if(bad_rating = 1,1,0))/sum(gmv_initial) as product_bad_rating,
                sum(gmv_initial*if(if_quality_refund = 1,1,0))/sum(gmv_initial) as product_quality_refunds,


                sum(gmv_initial*inaccurate_description)/sum(gmv_initial) as product_inaccurate_description,
                sum(gmv_initial*damage)/sum(gmv_initial) as product_damage,
                sum(gmv_initial*wrongs)/sum(gmv_initial) as product_wrongs,
                sum(gmv_initial*quality)/sum(gmv_initial) as product_quality,
                sum(gmv_initial*incomplete_package)/sum(gmv_initial) as product_incomplete_package,
                sum(gmv_initial*too_small_size)/sum(gmv_initial) as product_too_small_size,
                sum(gmv_initial*too_large_size)/sum(gmv_initial) as product_too_large_size,


                sum(gmv_initial*bad_quality_refund)/sum(gmv_initial) as product_bad_quality_refund,
                sum(gmv_initial*not_as_descibe_refund)/sum(gmv_initial) as product_not_as_descibe_refund,
                sum(gmv_initial*other_quality_refund)/sum(gmv_initial) as product_other_quality_refund

            from
                orders_with_feedback
                join
                    (select
                        product_id,
                        category_id
                    from
                        {{ source('mart', 'dim_published_product_min') }}
                    where
                        effective_ts <= '{{ var("start_date_ymd") }}'
                        and next_effective_ts > '{{ var("start_date_ymd") }}' ) using(product_id)
                left join {{ source('mart', 'category_levels') }} using(category_id)

            where
                datediff(delivered_time,created_time_utc) <= 80
                and partition_date between '{{ var("start_date_ymd") }}'  - interval 163 days and '{{ var("start_date_ymd") }}'  - interval 80 days
            group by 1,2,3
            order by 1,2,3),

        products_score as
        (select
            product_id,
            level_3_category_id,
            level_3_category_name,
            price_range,
            strata_NF_share_delivered as strata_nf_score,
            product_NF_share_delivered as product_nf_score,

            product_bad_rating - strata_bad_rating as bad_rating_diff,
            product_quality_refunds - strata_quality_refunds as quality_refunds_diff,

            product_inaccurate_description - strata_inaccurate_description as inaccurate_description_diff,
            product_damage - strata_damage as damage_diff,
            product_wrongs - strata_wrongs as wrongs_diff,
            product_quality - strata_quality as quality_diff,
            product_incomplete_package - strata_incomplete_package as incomplete_package_diff,
            product_too_small_size - strata_too_small_size as too_small_size_diff,
            product_too_large_size - strata_too_large_size as too_large_size_diff,

            product_bad_quality_refund - strata_bad_quality_refund as bad_quality_refund_diff,
            product_not_as_descibe_refund - strata_not_as_descibe_refund as not_as_descibe_refund_diff,
            product_other_quality_refund - strata_other_quality_refund as other_quality_refund_diff,


            product_orders,

            sum((greatest(strata_NF_share_delivered, 0.05) - product_NF_share_delivered)*product_orders)/sum(product_orders) as product_diff
        from
            product_metric join strats_metric
                using(level_3_category_id, level_3_category_name, price_range)
        group by 1,2,3,4,5,6,7,8,9,10, 11,12,13,14,15,16,17, 18, 19)

        select
            to_date('{{ var("start_date_ymd") }}') as partition_date,
            product_id,
            level_3_category_id,
            level_3_category_name,
            price_range,
            product_orders,
            if(product_orders < 15, 'unknown',
                if(t > 1.96, 'best',
                    if( t < -1.96, 'worst', 'average'))) as product_nf_segment,
            product_nf_score,
            strata_nf_score,
            bad_rating_diff,
            quality_refunds_diff,

            inaccurate_description_diff,
            damage_diff,
            wrongs_diff,
            quality_diff,
            incomplete_package_diff,
            too_small_size_diff,
            too_large_size_diff,

            bad_quality_refund_diff,
            not_as_descibe_refund_diff,
            other_quality_refund_diff

        from
            (select
               *,
               product_diff/(sqrt(product_nf_score*(1-product_nf_score)+0.000000000000001)/sqrt(product_orders)) as t
            from products_score)
