{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'false'
    }
) }}

SELECT _id AS                              order_product_id,
        id AS product_id,
       attachments AS attachments,
       currency AS currency,
       dangerKindSet AS danger_kind_set,
       dealId AS deal_id,
       description,
       disabled,
       duty,
       exportType AS export_type,
       friendlyId AS product_friendly_id,
       hsCode AS hs_code,
       link,
       linkToCalculation AS link_to_calculation,
       manufacturerId AS manufacturer_id,
       merchOrdId AS merchant_order_id,
       name AS name,
       nameRu AS name_ru,
       packaging,
       prices,
       productionDeadLine as production_dead_line,
       psiStatusID AS psi_status_id,
       status,
       statuses,
       trademark,
       type,
       variants,
       vatRate AS vat_rate,
       millis_to_ts_msk(ctms)  AS created_ts_msk
from {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
