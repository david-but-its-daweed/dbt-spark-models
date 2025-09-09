{% snapshot scd2_product_tier_snapshot %}

{{
    config(
    meta = {
      'model_owner' : '@daweed'
    }
      target_schema = 'b2b_mart',
      unique_key = 'product_id',
      
      strategy = 'check',
      check_cols = ['tier'],          
      invalidate_hard_deletes = True
    )
}}

WITH activity AS (
    SELECT
        FROM_JSON(e.event_params, 'product_id STRING').product_id AS product_id,
        current_date AS partition_date,
        COUNT_IF(e.event_type = 'productClick' AND DATE(event_time) >= CURRENT_DATE - INTERVAL 2 DAYS) AS views,
        COUNT_IF(e.event_type = 'addToCart' AND DATE(event_time) >= CURRENT_DATE - INTERVAL 2 DAYS) AS carts
    FROM {{ source('b2b_mart', 'ss_events_by_session' }},
        LATERAL VIEW explode(events_in_session) AS e
    WHERE 
        STARTSWITH(FROM_JSON(e.element.event_params, 'product_id STRING').product_id, '6') AND e.event_type IN ('productClick', 'addToCart')
    GROUP BY 1, 2
),

final AS (
    SELECT
        product_id,
        partition_date,
        CASE
            WHEN carts >= 1 THEN 'A'
            WHEN views >= 2 THEN 'B'
            ELSE NULL
        END AS tier
    FROM activity
)

SELECT
    product_id,
    tier
FROM final

{% endsnapshot %}
