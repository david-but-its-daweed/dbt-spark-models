{{ config(
    schema='customer_routing',
    materialized='view',
    meta = {
      'team': 'clan',
      'model_owner': '@ekutynina',
      'bigquery_load': 'true'
    }
) }}


WITH 
order_stg_1 AS 
(
    SELECT day,
         sale_type,
         gmv_initial,
         start_of_sale,
         end_of_sale,
         sale_period,
         lag_type,
         IF(lag_type != sale_type AND day >= start_of_sale - INTERVAL 7 DAY, day - INTERVAL 7 DAY, NULL) AS before_sale,
         IF(lag_type != sale_type AND day <= end_of_sale + INTERVAL 5 DAY, day + INTERVAL 5 DAY, NULL) AS after_sale 
    FROM (
        SELECT  day,
            sale_type,
            gmv_initial,
            MIN(COALESCE(start_of_sale, day)) OVER (PARTITION BY COALESCE(sale_type, day)) AS start_of_sale,
            MAX(COALESCE(end_of_sale, day)) OVER (PARTITION BY COALESCE(sale_type, day)) AS end_of_sale,
            sale_period,
            LAG(sale_type) OVER (ORDER BY day) AS lag_type
        FROM {{ ref('sales_data_daily') }} 
    )
),

order_stg_2 AS
(

    SELECT day,
         sale_type,
         gmv_initial,
         start_of_sale,
         end_of_sale,
         lag_type,
         before_sale,
         after_sale,
         sale_period,
         SUM(CASE WHEN before_sale IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY day) AS value_partition,
         LEAD(before_sale) OVER (ORDER BY day) AS before_sale_lead
    FROM order_stg_1
    
),

order_stg_3 AS
(
    SELECT 
        day,
        sale_type,
        gmv_initial,
        start_of_sale,
        end_of_sale,
        lag_type,
        value_partition,
        after_sale,
        sale_period,
        FIRST_VALUE(before_sale_lead) OVER (PARTITION BY value_partition ORDER BY DAY DESC) AS before_sale_lead,
        MIN(after_sale) OVER (PARTITION BY value_partition ORDER BY DAY) AS after_sale_first_date
    FROM order_stg_2
),

order_stg_4
(
    SELECT 
        day,
        sale_type,
        gmv_initial,
        start_of_sale,
        end_of_sale,
        sale_period,
        lag_type,
        sale_avg,
        IF(day >= before_sale_lead AND day < before_sale_lead + INTERVAL 7 DAY AND sale_type = "no_sales", avg_gmv_before_sale, NULL) AS avg_gmv_before_sale,
        IF(day < after_sale_first_date AND day >= after_sale_first_date - INTERVAL 5 DAY AND sale_type = "no_sales",  avg_gmv_after_sale, NULL) AS avg_gmv_after_sale        
    FROM (
        SELECT  
            day,
            sale_type,
            gmv_initial,
            start_of_sale,
            end_of_sale,
            sale_period,
            lag_type,
            before_sale_lead,
            after_sale_first_date,
            AVG(IF(day >= before_sale_lead AND day < before_sale_lead + INTERVAL 7 DAY AND sale_type = "no_sales",  gmv_initial, NULL)) OVER (PARTITION BY before_sale_lead) AS avg_gmv_before_sale,
            AVG(IF(day < after_sale_first_date   AND day >= after_sale_first_date - INTERVAL 5 DAY  AND sale_type = "no_sales",  gmv_initial, NULL)) OVER (PARTITION BY before_sale_lead) AS avg_gmv_after_sale,
            AVG(IF(sale_type != "no_sales", gmv_initial, NULL)) OVER (PARTITION BY sale_type) AS sale_avg        
        FROM order_stg_3
    )
),

order_stg_5
(
    SELECT 
        day,
        sale_type,
        gmv_initial,
        start_of_sale,
        end_of_sale,
        sale_period,
        sale_avg,
        FIRST_VALUE(avg_gmv_before_sale) OVER (PARTITION BY value_partition_before ORDER BY DAY) AS avg_gmv_before_sale_first,
        FIRST_VALUE(avg_gmv_after_sale) OVER (PARTITION BY value_partition_after ORDER BY DAY DESC) AS avg_gmv_after_sale_first
    FROM (
        SELECT 
            day,
            sale_type,
            gmv_initial,
            start_of_sale,
            end_of_sale,
            sale_period,
            avg_gmv_before_sale,
            avg_gmv_after_sale,
            sale_avg,
            SUM(CASE WHEN avg_gmv_before_sale IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY day) AS value_partition_before,
            SUM(CASE WHEN avg_gmv_after_sale IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY day DESC) AS value_partition_after 
        FROM order_stg_4
   )
),

order_stg_6 AS
(

    SELECT 
        sale_type,
        start_of_sale,
        end_of_sale,
        sale_period,
        SUM(sale_avg) AS sale_avg,
        MAX(avg_gmv_before_sale_first) AS avg_gmv_before_sale,
        MAX(avg_gmv_after_sale_first) AS avg_gmv_after_sale
    FROM order_stg_5
    GROUP BY 1, 2, 3, 4
)

SELECT sale_type,
        start_of_sale,
        end_of_sale,
        sale_period,
        sale_avg,
        avg_gmv_before_sale,
        avg_gmv_after_sale,
        sale_avg - avg_gmv_before_sale*sale_period AS sale_profit,
        IF(avg_gmv_before_sale > avg_gmv_after_sale, (sale_avg - avg_gmv_before_sale * sale_period) + 5*(avg_gmv_after_sale - avg_gmv_before_sale), sale_avg - avg_gmv_before_sale * sale_period) AS sale_proffit_with_hangover_5_day,  
        IF(avg_gmv_before_sale > avg_gmv_after_sale, (sale_avg - avg_gmv_before_sale * sale_period) + 5*(avg_gmv_after_sale - avg_gmv_before_sale), sale_avg - avg_gmv_before_sale * sale_period)/(avg_gmv_before_sale * sale_period) AS sale_proffit_with_hangover_5_day_pr,  
        IF(avg_gmv_before_sale > avg_gmv_after_sale, 1, 0) AS is_there_hangover,
        COUNT(IF(avg_gmv_before_sale > avg_gmv_after_sale, sale_period, NULL)) OVER (ORDER BY start_of_sale DESC) AS sales_with_hangover,
        COUNT(sale_period) OVER (ORDER BY start_of_sale DESC) AS sales
FROM order_stg_6
WHERE sale_type != 'no_sales'
ORDER BY start_of_sale
