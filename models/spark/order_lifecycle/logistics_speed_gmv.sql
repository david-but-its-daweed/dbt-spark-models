{{ config(
     schema='order_lifecycle',
     materialized='table',
     partition_by=['week_date'],
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'week_date',
       'alerts_channel': "#olc_dbt_alerts",
       'bigquery_fail_on_missing_partitions': 'false'
     }
 ) }}

with regions as (
    select 
        region, 
        explode(country_codes) country 
    from mart.dim_region
    where
        next_effective_ts >= "3000-01-01"
    union
    select 
        "DACH" region, 
        explode(array("DE", "AU", "CH")) country
    union
    select 
        "DACHFR" region, 
        explode(array("DE", "AU", "CH", "FR")) country
),

filter_logistics_mart as (
    select
        fo.order_id,
        fo.gmv_initial order_gmv,
        r.region,
        fo.order_created_date_utc,
        date_trunc('week', to_timestamp(fo.order_created_date_utc)) as order_created_week,
        to_timestamp(fo.tracking_destination_country_time_utc) destination_country_date,
        date_trunc('week', to_timestamp(fo.tracking_destination_country_time_utc)) as destination_country_week,
        fo.shipping_type,
        round(fo.delivery_duration_tracking) as delivery_duration_tracking,
        round(datediff(fo.tracking_destination_country_time_utc, fo.order_created_time_utc)) as delivery_duration_destination,
        round(datediff(fo.tracking_issuing_point_time_utc, fo.tracking_destination_country_time_utc)) as delivery_duration_from_destination,
        if(fo.tracking_destination_country_time_utc is not null and (fo.refund_type is null or fo.refund_type != "not_delivered") or fo.delivery_duration_tracking is not null, 1, 0) is_data_exist, --заказы для которых сможем посчитать скорость
        if(fo.refund_type = "not_delivered", 1 , 0) is_refunded_not_deliv
    from 
        {{ source('logistics_mart', 'fact_order') }} fo
    left join
        regions r
        using(country)
    where 
        (fo.refund_type is null 
        OR fo.refund_type NOT IN ('cancelled_by_customer', 'cancelled_by_merchant'))
        and fo.order_created_date_utc >= "2020-01-01"
        and fo.origin_country = 'CN'
        and fo.shipping_type IN ('RM', 'SRM', 'NRM')
),

t1 as ( -- считаем avg_delivery_duration_from_destination - средняя длительнось внутри страны для рм
    select
        region,
        destination_country_week,
        round(avg(delivery_duration_from_destination)) as avg_delivery_duration_from_destination
    from filter_logistics_mart
    where 
        destination_country_week is not null 
        and delivery_duration_from_destination is not null
    group by 1, 2
),

data_stat as (-- считаем долю gmv заказов, по которым можем посчитать длительность
    select
        order_created_week,
        region,
        avg(is_data_exist) share_data_exist,
        avg(if(is_data_exist = 0 and is_refunded_not_deliv = 0, 1, 0)) share_not_data_not_refunded,
        avg(if(is_data_exist = 0 and is_refunded_not_deliv = 1, 1, 0)) share_not_data_refunded
    from
        filter_logistics_mart l
    group by 1, 2
),

existed_data as (-- считаем general_duration - для рм по трекингу, для нрм складываем длительность до страны назначения со средней длительности внутри страны для рм для дня доставки в страну
    select
        l.*,
        t1.avg_delivery_duration_from_destination,
        coalesce(l.delivery_duration_tracking, l.delivery_duration_destination + t1.avg_delivery_duration_from_destination) general_duration
    from
        filter_logistics_mart l
    left join
        t1
        on l.region = t1.region
        and l.destination_country_week = t1.destination_country_week
    where
        l.is_data_exist = 1
),

orders_by_delivery_days as (
    select
        region,
        order_created_date_utc,
        general_duration,
        count(order_id) as order_count,
        sum(order_gmv) as order_gmv,
        sum(is_refunded_not_deliv) as refund_deliv_count,
        sum(is_refunded_not_deliv * order_gmv) as refund_deliv_gmv
    from existed_data
    group by 1, 2, 3
),

add_orders_before_dd as (
    select
        region,
        order_created_date_utc,
        general_duration,
        sum(order_gmv) OVER(PARTITION BY region, order_created_date_utc ORDER BY general_duration) as order_gmv_before_dd,
        sum(order_gmv) OVER(PARTITION BY region, order_created_date_utc) as total_order_gmv,
        sum(refund_deliv_gmv) OVER(PARTITION BY region, order_created_date_utc) as total_refund_deliv_gmv
    from orders_by_delivery_days
),

add_perc as(
    select
        region,
        order_created_date_utc,
        sum(total_refund_deliv_gmv) total_refund_deliv_gmv,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.05, general_duration, null)) as perc_5_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.1, general_duration, null)) as perc_10_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.25, general_duration, null)) as perc_25_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.5, general_duration, null)) as perc_50_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.8, general_duration, null)) as perc_80_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.9, general_duration, null)) as perc_90_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.95, general_duration, null)) as perc_95_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.99, general_duration, null)) as perc_99_delivered,
        min(if(order_gmv_before_dd * 1.0 / total_order_gmv >= 1.0, general_duration, null)) as perc_100_delivered
    from add_orders_before_dd
    group by 1, 2
)


select
    add_perc.region, 
    date(date_trunc('week', add_perc.order_created_date_utc)) as week_date,
    round(avg(add_perc.perc_5_delivered)) AS perc_5_delivered,
    round(avg(add_perc.perc_10_delivered)) AS perc_10_delivered,
    round(avg(add_perc.perc_25_delivered)) AS perc_25_delivered,
    round(avg(add_perc.perc_50_delivered)) AS perc_50_delivered,
    round(avg(add_perc.perc_80_delivered)) AS perc_80_delivered,
    round(avg(add_perc.perc_90_delivered)) AS perc_90_delivered,
    round(avg(add_perc.perc_95_delivered)) AS perc_95_delivered,
    round(avg(add_perc.perc_99_delivered)) AS perc_99_delivered,
    round(avg(add_perc.perc_100_delivered)) AS perc_100_delivered,
    min(data_stat.share_data_exist) AS share_data_exist,
    min(data_stat.share_not_data_not_refunded) share_not_data_not_refunded,
    min(data_stat.share_not_data_refunded) share_not_data_refunded
from add_perc
left join data_stat
    on add_perc.region = data_stat.region
    and date_trunc('week', add_perc.order_created_date_utc) = data_stat.order_created_week
group by 1, 2
order by 2
