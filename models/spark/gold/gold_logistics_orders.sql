{{
  config(
    materialized='incremental',
    alias='logistics_orders',
    schema='gold',
    file_format='delta',
    incremental_strategy='insert_overwrite',
    partition_by=['order_date_msk'],
    on_schema_change='sync_all_columns',
    meta = {
        'model_owner' : '@analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'bigquery_partitioning_date_column': 'order_date_msk',
        'bigquery_upload_horizon_days': '365',
        'priority_weight': '1000',
    }
  )
}}

SELECT
    a.order_id,
    a.order_number AS shipping_order_number,
    a.order_group_id,
    a.parcel_id,
    a.tracking_number,

    a.user_id,
    a.device_id,

    IF(a.country = '', NULL, a.country) AS country_code,
    COALESCE(c.top_country_code, 'Other') AS top_country_code,
    COALESCE(c.country_priority_type, 'Other') AS country_priority_type,
    COALESCE(c.region_name, 'Other') AS region_name,
    a.origin_name,
    a.is_online_shipping AS is_delivered_by_jl,
    IF(
        TRUE
        AND a.is_fbj_order
        AND a.warehouse_country = 'CN'
        AND a.order_created_date_utc >= DATE '2024-05-01',
        TRUE,
        FALSE
    ) AS is_fbj,
    a.delivery_method_name,
    a.linehaul_shipper AS linehaul_shipper_final,
    a.initial_shipping_type AS shipping_type_initial,
    a.shipping_type AS shipping_type_final,
    a.initial_channel_id AS channel_id_initial,
    a.channel_id AS channel_id_final,
    a.category_id AS merchant_category_id,
    mc.business_line,
    a.warehouse_country AS warehouse_country_code,
    a.merchant_id,

    a.is_consolidated OR a.consolidation_group_id IS NOT NULL AS is_planned_for_consolidation_by_hecny,
    a.is_consolidated AS is_consolidated_by_hecny,
    a.is_consolidated_by_merchant,
    a.is_refunded,
    a.refund_type AS refund_reason,

    a.order_weight AS shipping_order_weight,
    a.parcel_weight,
    a.quantity AS product_quantity,

    a.gmv_initial,
    a.gmv_refund,
    a.final_revenue_usd AS jl_revenue_final,
    a.final_consolidation_revenue AS jl_consolidation_revenue_final,
    a.final_gross_profit_usd AS jl_gross_profit_final,
    a.final_consolidation_profit_usd AS jl_consolidation_profit_final,
    a.final_total_cost_usd AS jl_total_cost_final,

    a.warranty_duration AS days_to_non_delivery_warranty_start,
    a.warranty_duration_max AS days_to_non_delivery_warranty_end,
    a.warranty_start_dt AS warranty_start_date_msk,
    a.warranty_end_dt AS warranty_end_date_msk,

    a.delivery_estimate_min_days AS delivery_estimate_lower_bound_days,
    a.delivery_estimate_max_days AS delivery_estimate_upper_bound_days,
    a.delivery_duration_user AS delivery_duration_by_user,
    a.delivery_duration_tracking AS delivery_duration_by_tracking,
    ROUND((UNIX_SECONDS(a.check_in_time_utc) - UNIX_SECONDS(a.order_created_time_utc)) / 60 / 60 / 24, 1) AS fulfilling_duration,

    a.order_created_date_msk AS order_date_msk,
    a.order_created_time_utc AS order_created_datetime_utc,
    CAST(DATEADD(HOUR, 3, a.order_fulfilled_online_time_utc) AS DATE) AS order_fulfilled_online_date_msk,
    a.check_in_time_utc AS check_in_datetime_utc,
    a.consolidation_in_checkpoint_time_utc,
    a.consolidation_out_checkpoint_time_utc,
    a.check_out_time_utc AS jl_costs_realization_datetime_utc,
    a.outbound_time_utc AS outbound_datetime_utc,
    a.tracking_origin_country_time_utc AS tracking_origin_country_datetime_utc,
    a.tracking_in_transit_time_utc AS tracking_in_transit_datetime_utc,
    a.tracking_international_time_utc AS tracking_international_datetime_utc,
    a.tracking_destination_country_time_utc AS tracking_destination_country_datetime_utc,
    a.tracking_issuing_point_time_utc AS tracking_issuing_point_datetime_utc,
    a.tracking_delivered_time_utc AS tracking_delivered_datetime_utc,
    a.tracking_returned_time_utc AS tracking_returned_datetime_utc,
    a.tracking_status AS logistics_tracking_stage
FROM {{ source('logistics_mart', 'fact_order') }} AS a
LEFT JOIN {{ ref('gold_countries') }} AS c ON a.country = c.country_code
LEFT JOIN {{ ref('gold_merchant_categories') }} AS mc ON a.category_id = mc.merchant_category_id
{% if is_incremental() %}
    WHERE a.order_created_date_msk >= DATE '{{ var("start_date_ymd") }}' - INTERVAL 365 DAYS
{% endif %}
DISTRIBUTE BY order_created_date_msk