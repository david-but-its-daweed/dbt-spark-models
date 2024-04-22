{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}


WITH fb AS (
    SELECT
        campaign_name,
        'facebook' AS source,
        DATE_TRUNC('day', date) AS date,
        SUM(impressions) AS impressions,
        SUM(spend) AS spend,
        SUM(clicks) AS clicks,
        SUM(clicks) / SUM(impressions) AS ctr,
        SUM(spend) / SUM(clicks) AS cpc
    FROM
        {{ source('pharmacy', 'fbads_adset_country_insights') }}
    WHERE
        1 = 1
        AND date >= '2022-01-01'
    GROUP BY
        campaign_name,
        DATE_TRUNC('day', date)
)

SELECT *
FROM fb
