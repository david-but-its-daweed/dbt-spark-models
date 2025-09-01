{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


SELECT
    _id AS shipment_order_id,
    friendlyId AS shipment_order_friendly_id,
    address,
    boxOrdering AS box_ordering,
    broker,
    comment,
    country,
    destinationPort AS destination_port,
    fmpId AS fmp_id,
    hsShipmentId AS hs_shipment_id,
    hsSource AS hs_source,
    plannedDate AS planned_date,
    readyForPickupDate AS ready_for_pickup_date,
    shipmentDocsFolderId AS shipment_docs_folder_id,
    shippedDate AS shipped_date,
    statusHistory AS status_history,
    tags,
    v,
    MILLIS_TO_TS_MSK(ctms) AS created_time,
    MILLIS_TO_TS_MSK(utms) AS updated_time
FROM {{ source('mongo', 'b2b_core_shipment_orders_daily_snapshot') }}
