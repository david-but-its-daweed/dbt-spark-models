{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

SELECT "PickupRequestSentToLogisticians" as stage,
1000 as stage_int
UNION ALL
SELECT "PickedUpByLogisticians" as stage,
1010 as stage_int
UNION ALL
SELECT "ArrivedAtLogisticsWarehouse" as stage,
1020 as stage_int
UNION ALL
SELECT "DepartedFromLogisticsWarehouse" as stage,
1030 as stage_int
UNION ALL
SELECT "ArrivedAtStateBorder" as stage,
1040 as stage_int
UNION ALL
SELECT "DepartedFromStateBorder" as stage,
1050 as stage_int
UNION ALL
SELECT "ArrivedAtDestanationPoint" as stage,
1060 as stage_int
UNION ALL
SELECT "DocumentReceived" as stage,
1070 as stage_int
UNION ALL
SELECT "CustomsDeclarationFiled" as stage,
1080 as stage_int
UNION ALL
SELECT "CustomsDeclarationReleased" as stage,
1090 as stage_int
UNION ALL
SELECT "UploadToTemporaryWarehouse" as stage,
1100 as stage_int
UNION ALL
SELECT "Delivering" as stage,
1110 as stage_int
UNION ALL
SELECT "Delivered" as stage,
1120 as stage_int
