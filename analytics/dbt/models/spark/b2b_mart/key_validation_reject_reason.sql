{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


SELECT
    1010 AS id,
    'noFeedback' AS reason
UNION ALL
SELECT
    1020 AS id,
    'unsuitableDeliveryTerms' AS reason
UNION ALL
SELECT
    1030 AS id,
    'unsuitableFinancialTerms' AS reason
UNION ALL
SELECT
    1040 AS id,
    'unsuitableWarehouseTerms' AS reason
UNION ALL
SELECT
    1050 AS id,
    'unsuitableVolume' AS reason
UNION ALL
SELECT
    1060 AS id,
    'needsTimeForDecision' AS reason
UNION ALL
SELECT
    1070 AS id,
    'didNotSendRequest' AS reason
UNION ALL
SELECT
    1080 AS id,
    'deadRequest' AS reason
UNION ALL
SELECT
    1090 AS id,
    'uncommunicativeClient' AS reason
UNION ALL
SELECT
    1100 AS id,
    'noLegalEntity' AS reason
UNION ALL
SELECT
    1110 AS id,
    'other' AS reason
