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
"New" AS sub_status
UNION ALL
SELECT
1020 AS id,
"PriceEstimation" AS sub_status
UNION ALL
SELECT
1030 AS id,
"Negotiation" AS sub_status
UNION ALL
SELECT
1040 AS id,
"FinalPricing" AS sub_status
UNION ALL
SELECT
1050 AS id,
"SigningAndPayment" AS sub_status
UNION ALL
SELECT
2010 AS id,
"Client2BrokerPaymentSent" AS sub_status
UNION ALL
SELECT
2020 AS id,
"BrokerPaymentReceived" AS sub_status
UNION ALL
SELECT
2030 AS id,
"Broker2JoomSIAPaymentSent" AS sub_status
UNION ALL
SELECT
2049 AS id,
"JoomSIAPaymentReceived" AS sub_status
UNION ALL
SELECT
3010 AS id,
"PickupRequestSentToLogisticians" AS sub_status
UNION ALL
SELECT
3020 AS id,
"PickedUpByLogisticians" AS sub_status
UNION ALL
SELECT
3030 AS id,
"ArrivedAtLogisticsWarehouse" AS sub_status
UNION ALL
SELECT
3040 AS id,
"DepartedFromLogisticsWarehouse" AS sub_status
UNION ALL
SELECT
3050 AS id,
"ArrivedAtStateBorder" AS sub_status
UNION ALL
SELECT
3060 AS id,
"DepartedFromStateBorder" AS sub_status
UNION ALL
SELECT
3065 AS id,
"ArrivedAtDestanationPoint" AS sub_status
UNION ALL
SELECT
3070 AS id,
"DocumentReceived" AS sub_status
UNION ALL
SELECT
3080 AS id,
"CustomsDeclarationFiled" AS sub_status
UNION ALL
SELECT
3090 AS id,
"CustomsDeclarationReleased" AS sub_status
UNION ALL
SELECT
3093 AS id,
"UploadToTemporaryWarehouse" AS sub_status
UNION ALL
SELECT
3097 AS id,
"Delivering" AS sub_status
UNION ALL
SELECT
3100 AS id,
"Delivered" AS sub_status
UNION ALL
SELECT
6010 AS id,
"Duplicate" AS sub_status
UNION ALL
SELECT
6020 AS id,
"UnsuitableDeliveryTerms" AS sub_status
UNION ALL
SELECT
6030 AS id,
"UnsuitableVolume" AS sub_status
UNION ALL
SELECT
6040 AS id,
"UnsuitableProduct" AS sub_status
UNION ALL
SELECT
6050 AS id,
"UnsuitableDimensions" AS sub_status
UNION ALL
SELECT
6060 AS id,
"DeadRequest" AS sub_status
UNION ALL
SELECT
6070 AS id,
"ProductNotFound" AS sub_status
UNION ALL
SELECT
6080 AS id,
"TotalPriceTooHigh" AS sub_status
UNION ALL
SELECT
6090 AS id,
"MerchantPriceTooHigh" AS sub_status
UNION ALL
SELECT
6100 AS id,
"LogisticsPriceTooHigh" AS sub_status
UNION ALL
SELECT
6110 AS id,
"ApprovalDocPriceTooHigh" AS sub_status
UNION ALL
SELECT
6120 AS id,
"LegalizationPriceTooHigh" AS sub_status
UNION ALL
SELECT
6130 AS id,
"ClientDoesNotHaveEnoughMoney" AS sub_status
UNION ALL
SELECT
6131 AS id,
"CancelledByJoomPro" AS sub_status
UNION ALL
SELECT
6140 AS id,
"Other" AS sub_status
