{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH procurement_orders AS (
    SELECT _id AS procurement_order_id,
           friendlyId AS procurement_order_friendly_id,
           dealId AS deal_id,
           dealType AS deal_type,
           country,
           CASE WHEN isSmallBatch IS TRUE THEN 1 ELSE 0 END AS is_small_batch,
           coreEmpty AS core_empty,
           id AS product_id,
           name AS product_name,
           procurementStatuses AS procurement_statuses,
           psiStatusID AS current_psi_status_id_long,
           manufacturerId AS manufacturer_id,
           CASE WHEN manDaysFilled IS TRUE THEN manDays END AS manufacturing_days,
           productionRange AS production_range,
           warehouse,
           TO_DATE(SUBSTR(minPickupDate, 1, 8), 'yyyyMMdd') AS min_pickup_date,
           merchOrdId AS merchant_order_id,
           jpcPayment AS jpc_payment,
           payment,
           nullIf(size(payment.paymentScheme), 0) AS payment_count_plan,
           prices,
           productRoles AS product_roles,
           variants AS procurement_order_variants,
           millis_to_ts_msk(ctms) AS created_ts,
           millis_to_ts_msk(utms) AS updated_ts
    FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }} AS op
    WHERE isDeleted IS NOT TRUE
      AND country IN ('BR', 'KZ')
),

     filtered_payments AS (
    WITH billing_info AS (
        SELECT t1._id AS payment_id,
               t1.isCancelled AS is_payment_cancelled
        FROM mongo.billing_pro_invoice_requests_daily_snapshot AS t1
        /*
        LEFT JOIN mongo.billing_pro_invoice_request_operations_daily_snapshot AS t2 ON t1._id = t2.requestId
        LEFT JOIN mongo.billing_pro_invoices_v3_daily_snapshot AS t3 ON t2.invoiceId = t3._id
        LEFT JOIN mongo.billing_pro_payouts_daily_snapshot AS t4 ON t3.pId = t4._id
        */
    )


    SELECT
        procurement_order_id,
        NAMED_STRUCT(
            'advancePercent', advancePercent,
            'daysAfterQC', daysAfterQC,
            'paymentHistory', 
                COLLECT_LIST(
                    NAMED_STRUCT(
                        'id', id,
                        'ctms', ctms,
                        'utms', utms,
                        'price', price
                    )
                ),
            'paymentScheme', paymentScheme,
            'paymentType', paymentType,
            'pmId', pmId,
            'workScheme', workScheme
        ) AS payment
    FROM (
        SELECT
            po.procurement_order_id,
            po.payment.advancePercent,
            po.payment.daysAfterQC,
            po.payment.paymentScheme,
            po.payment.paymentType,
            po.payment.pmId,
            po.payment.workScheme,
            ph.id,
            ph.ctms,
            ph.utms,
            ph.price
        FROM procurement_orders AS po
        LATERAL VIEW EXPLODE(po.payment.paymentHistory) AS ph
    ) AS p
    LEFT JOIN billing_info AS bi ON p.id = bi.payment_id
    WHERE COALESCE(bi.is_payment_cancelled, False) IS NOT TRUE
    GROUP BY
        procurement_order_id,
        advancePercent,
        daysAfterQC,
        paymentScheme,
        paymentType,
        pmId,
        workScheme
),

     payments_history AS (
    SELECT procurement_order_id,
           COUNT(p.payment_id) AS payment_count_fact,
           COUNT(CASE WHEN payment_received_ts_msk IS NOT NULL THEN p.payment_id END) AS received_payment_count,
           COUNT(CASE WHEN sla_fact > sla_plan THEN p.payment_id END) AS overdue_payment_count,
           SUM(CASE WHEN payment_price_ccy = 'USD' THEN payment_price_amount ELSE 0 END) AS payment_sum_usd,
           SUM(CASE WHEN payment_price_ccy = 'CNH' THEN payment_price_amount ELSE 0 END) AS payment_sum_cnh,
           SUM(CASE WHEN payment_price_ccy NOT IN('USD', 'CNH') THEN payment_price_amount ELSE 0 END) AS payment_sum_other
    FROM (
        SELECT po.procurement_order_id,
               ph.id AS payment_id,
               ph.price.amount / 1000000 AS payment_price_amount,
               ph.price.ccy AS payment_price_ccy,
               millis_to_ts_msk(ph.ctms) AS payment_requested_ts_msk,
               millis_to_ts_msk(ph.utms) AS payment_received_ts_msk,
               DATEDIFF(millis_to_ts_msk(ph.utms), millis_to_ts_msk(ph.ctms)) AS sla_fact,
               4 AS sla_plan
        FROM filtered_payments AS po
        LATERAL VIEW EXPLODE(po.payment.paymentHistory) AS ph
    ) AS p
    GROUP BY 1
),

     procurement_orders_last_assignee AS (
    WITH assignee_history AS (
        SELECT _id AS procurement_order_id,
               role_key AS role,
               role_value.type AS role_type,
               role_value.moderatorId AS assignee_id,
               millis_to_ts_msk(role_value.updatedTime) AS assignee_ts,
               ROW_NUMBER() OVER (PARTITION BY _id ORDER BY role_value.updatedTime DESC) AS assignee_row_n_desc
        FROM {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
        LATERAL VIEW EXPLODE(productRoles.roles) exploded_roles AS role_key, role_value
    )

    SELECT ah.procurement_order_id,
           ah.role AS assignee_role,
           ah.assignee_id,
           au.email AS assignee_email,
           ah.assignee_ts
    FROM assignee_history AS ah
    LEFT JOIN {{ source('mongo', 'b2b_core_admin_users_daily_snapshot') }} AS au ON ah.assignee_id = au._id
    WHERE assignee_row_n_desc = 1
),

     procurement_statuses_history AS (
    SELECT procurement_order_id,
           MIN(status_time) AS first_status_ts,
           /* Current Status and Sub Status */
           MAX_BY(procurement_status_name, status_time) AS current_status,
           MAX_BY(procurement_sub_status_name, status_time) AS current_sub_status,
           MAX(status_time) AS current_status_ts,
           /* Status */
           MIN(CASE WHEN procurement_status_name = 'preProcessing' THEN status_time END) AS status_pre_processing_ts,
           MIN(CASE WHEN procurement_status_name = 'formingOrder' THEN status_time END) AS status_forming_order_ts,
           MIN(CASE WHEN procurement_status_name = 'awaitingForClientConfirmation' THEN status_time END) AS status_awaiting_for_client_confirmation_ts,
           MIN(CASE WHEN procurement_status_name = 'ali1688' THEN status_time END) AS status_ali_1688_ts,
           MIN(CASE WHEN procurement_status_name = 'receiving' THEN status_time END) AS status_receiving_ts,
           MIN(CASE WHEN procurement_status_name = 'preparingOrder' THEN status_time END) AS status_preparing_order_ts,
           MIN(CASE WHEN procurement_status_name = 'signingWithMerchant' THEN status_time END) AS status_signing_with_merchant_ts,
           MIN(CASE WHEN procurement_status_name = 'payment' THEN status_time END) AS status_payment_ts,
           MIN(CASE WHEN procurement_status_name = 'manufacturing' THEN status_time END) AS status_manufacturing_ts,
           MIN(CASE WHEN procurement_status_name = 'psi' THEN status_time END) AS status_psi_ts,
           MIN(CASE WHEN procurement_status_name = 'packingAndLabeling' THEN status_time END) AS status_packing_and_labeling_ts,
           MIN(CASE WHEN procurement_status_name = 'readyForShipment' THEN status_time END) AS status_ready_for_shipment_ts,
           MIN(CASE WHEN procurement_status_name = 'shippedBy3PL' THEN status_time END) AS status_shipped_by_3pl_ts,
           MIN(CASE WHEN procurement_status_name = 'pickUpPayment' THEN status_time END) AS status_pick_up_payment_ts,
           MIN(CASE WHEN procurement_status_name = 'completed' THEN status_time END) AS status_completed_ts,
           MIN(CASE WHEN procurement_status_name = 'cancelled' THEN status_time END) AS status_cancelled_ts,
           /* Sub Status */
           MIN(CASE WHEN procurement_sub_status_name = 'preProcessing' THEN status_time END) AS sub_status_pre_processing_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'formingOrderUnassigned' THEN status_time END) AS sub_status_forming_order_unassigned_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'fillingInInformation' THEN status_time END) AS sub_status_filling_in_information_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'confirmedByProcurement' THEN status_time END) AS sub_status_confirmed_by_procurement_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'awaitingForClientConfirmation' THEN status_time END) AS sub_status_awaiting_for_client_confirmation_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'waitingForPayment' THEN status_time END) AS sub_status_waiting_for_payment_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'merchantPreparingOrder' THEN status_time END) AS sub_status_merchant_preparing_order_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'merchantShippedTheGoods' THEN status_time END) AS sub_status_merchant_shipped_the_goods_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'issues' THEN status_time END) AS sub_status_issues_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'preparingOrder' THEN status_time END) AS sub_status_preparing_order_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'awaitingForSigning' THEN status_time END) AS sub_status_awaiting_for_signing_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'merchantSigned' THEN status_time END) AS sub_status_merchant_signed_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'clientPaymentReceived' THEN status_time END) AS sub_status_client_payment_received_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'advancePaymentRequested' THEN status_time END) AS sub_status_advance_payment_requested_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'advancePaymentInProgress' THEN status_time END) AS sub_status_advance_payment_in_progress_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'manufacturing' THEN status_time END) AS sub_status_manufacturing_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'psiBeingConducted' THEN status_time END) AS sub_status_psi_being_conducted_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'psiIsPlanned' THEN status_time END) AS sub_status_psi_is_planned_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'psiWaitingForConfirmation' THEN status_time END) AS sub_status_psi_waiting_for_confirmation_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'psiProblemsAreToBeFixed' THEN status_time END) AS sub_status_psi_problems_are_to_be_fixed_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'psiResultsAccepted' THEN status_time END) AS sub_status_psi_results_accepted_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'psiApprovedManually' THEN status_time END) AS sub_status_psi_approved_manually_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'packingAndLabeling' THEN status_time END) AS sub_status_packing_and_labeling_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'readyForShipment' THEN status_time END) AS sub_status_ready_for_shipment_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'shippedBy3PL' THEN status_time END) AS sub_status_shipped_by_3pl_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'finalPaymentRequested' THEN status_time END) AS sub_status_final_payment_requested_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'finalPaymentInProgress' THEN status_time END) AS sub_status_final_payment_in_progress_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'finalPaymentAcquired' THEN status_time END) AS sub_status_final_payment_acquired_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'pickUpPaymentPickedUp' THEN status_time END) AS sub_status_pick_up_payment_picked_up_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'completed' THEN status_time END) AS sub_status_completed_ts,
           MIN(CASE WHEN procurement_sub_status_name = 'cancelled' THEN status_time END) AS sub_status_cancelled_ts
    FROM (
        SELECT ps.*,
               ks1.name AS procurement_status_name,
               ks2.name AS procurement_sub_status_name /* preparingOrder - это Awaiting for KAM */
        FROM (
            SELECT po.procurement_order_id,
                   ps.comment,
                   ps.status,
                   ps.subStatus,
                   millis_to_ts_msk(ps.statusTime) AS status_time
            FROM procurement_orders AS po
            LATERAL VIEW EXPLODE(po.procurement_statuses) AS ps
        ) AS ps
        LEFT JOIN {{ ref('key_status') }} AS ks1 ON ks1.key = 'orderproduct.procurementStatus' AND ps.status = ks1.id
        LEFT JOIN {{ ref('key_status') }} AS ks2 ON ks2.key = 'orderproduct.procurementSubStatus' AND ps.subStatus = ks2.id
    ) AS ps
    GROUP BY 1
),

     psi_history AS (
    WITH psi AS (
        SELECT _id AS psi_status_id_long,
               LAST_VALUE(_id) OVER (
                   PARTITION BY get_json_object(context, '$.moId'), get_json_object(context, '$.pId')
                   ORDER BY millis_to_ts_msk(stms)
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS current_psi_status_id_long,
               statusId AS psi_status_id,
               CASE statusId
                   WHEN 10 THEN 'PSIStatusWaiting'
                   WHEN 20 THEN 'PSIStatusRunning'
                   WHEN 30 THEN 'PSIStatusReady'
                   WHEN 40 THEN 'PSIStatusFail'
                   WHEN 50 THEN 'PSIStatusSuccess'
               END AS psi_status,
               get_json_object(context, '$.moId') AS merchant_order_id,
               get_json_object(context, '$.pId') AS product_id,
               millis_to_ts_msk(stms) AS status_ts,
               payloadNew AS payload_new
        FROM {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
    ),

         psi_problems AS (
        SELECT psi_status_id_long,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'goodQuality' THEN 1 ELSE 0 END) AS quality,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'goodQuality' THEN 1 ELSE 0 END) AS client_quality,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'customsRequirements' THEN 1 ELSE 0 END) AS customs,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'customsRequirements' THEN 1 ELSE 0 END) AS client_customs,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'logisticsRequirements' THEN 1 ELSE 0 END) AS logistics,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'logisticsRequirements' THEN 1 ELSE 0 END) AS client_logistics,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'discrepancy' THEN 1 ELSE 0 END) AS discrepancy,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'discrepancy' THEN 1 ELSE 0 END) AS client_discrepancy,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'clientsRequirements' THEN 1 ELSE 0 END) AS requirements,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'clientsRequirements' THEN 1 ELSE 0 END) AS client_requirements,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'other' THEN 1 ELSE 0 END) AS other,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'other' THEN 1 ELSE 0 END) AS client_other,
               MAX(CASE WHEN name = 'problems' AND problem.value = 'other' THEN problem.comment END) AS comment,
               MAX(CASE WHEN name = 'failureProblems' AND problem.value = 'other' THEN problem.comment END) AS client_comment
        FROM (
            SELECT psi_status_id_long,
                   col.name,
                   EXPLODE(col.enumPayload.selectedItems) AS problem
            FROM (
                SELECT _id AS psi_status_id_long,
                       EXPLODE(payloadNew)
                FROM {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
                WHERE payloadNew IS NOT NULL
            )
            WHERE col.name IN ('problems', 'failureProblems')
        )
        GROUP BY 1
    ),
    
         psi_inspection AS (
        SELECT psi_status_id_long,
               col.datePayload.value,
               TIMESTAMP(col.datePayload.value) + INTERVAL 3 HOURS AS inspection_ts
        FROM (
            SELECT _id AS psi_status_id_long,
                   EXPLODE(payloadNew)
            FROM {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
            WHERE payloadNew IS NOT NULL
        )
        WHERE col.type = 'date' 
          AND col.name = 'dateOfInspection' 
          AND col.datePayload IS NOT NULL
    ),
    
         psi_solution AS (
        SELECT psi_status_id_long,
               col.enumPayload.selectedItems[0].value AS solution
        FROM (
            SELECT _id AS psi_status_id_long,
                   EXPLODE(payloadNew)
            FROM {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
            WHERE payloadNew IS NOT NULL
        )
        WHERE col.name = 'solution'
    )


    SELECT current_psi_status_id_long,
           MAX_BY(psi_status, status_ts) AS current_psi_status,
           MAX(status_ts) AS current_psi_status_ts,
           MIN(CASE WHEN psi_status = 'PSIStatusWaiting' THEN status_ts END) AS psi_waiting_ts,
           MIN(CASE WHEN psi_status = 'PSIStatusRunning' THEN status_ts END) AS psi_running_ts,
           MIN(CASE WHEN psi_status = 'PSIStatusReady' THEN status_ts END) AS psi_ready_ts,
           MIN(CASE WHEN psi_status = 'PSIStatusFail' THEN status_ts END) AS psi_fail_ts,
           MAX(CASE WHEN row_n = 1 AND psi_status = 'PSIStatusSuccess' THEN status_ts END) AS psi_success_ts,
           MIN(CASE WHEN row_n = 1 THEN inspection_ts END) AS inspection_ts,
           MAX(CASE WHEN row_n = 1 THEN solution END) AS solution,
           MAX(CASE WHEN row_n = 1 THEN quality END) AS problem_quality,
           MAX(CASE WHEN row_n = 1 THEN client_quality END) AS problem_client_quality,
           MAX(CASE WHEN row_n = 1 THEN customs END) AS problem_customs,
           MAX(CASE WHEN row_n = 1 THEN client_customs END) AS problem_client_customs,
           MAX(CASE WHEN row_n = 1 THEN logistics END) AS problem_logistics,
           MAX(CASE WHEN row_n = 1 THEN client_logistics END) AS problem_client_logistics,
           MAX(CASE WHEN row_n = 1 THEN discrepancy END) AS problem_discrepancy,
           MAX(CASE WHEN row_n = 1 THEN client_discrepancy END) AS problem_client_discrepancy,
           MAX(CASE WHEN row_n = 1 THEN requirements END) AS problem_requirements,
           MAX(CASE WHEN row_n = 1 THEN client_requirements END) AS problem_client_requirements,
           MAX(CASE WHEN row_n = 1 THEN other END) AS problem_other,
           MAX(CASE WHEN row_n = 1 THEN client_other END) AS problem_client_other,
           MAX(CASE WHEN row_n = 1 THEN comment END) AS problem_comment,
           MAX(CASE WHEN row_n = 1 THEN client_comment END) AS problem_client_comment
    FROM (
        SELECT *,
               /* В PSI может быть несколько итераций проверок, поэтому оставляем данные только первой итерации */
               ROW_NUMBER() OVER (PARTITION BY current_psi_status_id_long, psi_status ORDER BY status_ts) AS row_n
        FROM psi
    ) AS p
    LEFT JOIN psi_inspection AS pp USING (psi_status_id_long)
    LEFT JOIN psi_solution AS pp USING (psi_status_id_long)
    LEFT JOIN psi_problems AS pp USING (psi_status_id_long)
    GROUP BY 1
),

     merchant_orders AS (
    SELECT mo._id AS merchant_order_id,
           mo.friendlyId AS merchant_order_friendly_id,
           mo.orderId AS client_order_id,
           mo.merchantId AS merchant_id,
           m.Name AS merchant_name,
           mo.manDays AS manufacturing_days
    FROM {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }} AS mo
    LEFT JOIN {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }} AS m ON mo.merchantId = m._id
    WHERE mo.deleted IS NOT TRUE
),

     offer_products AS (
    SELECT _id AS procurement_order_id,
           offerId AS customer_offer_id,
           id AS product_id,
           name AS product_name,
           manufacturerId AS manufacturer_id,
           variants AS offer_products_variants,
           millis_to_ts_msk(ctms) AS offer_product_created_ts
    FROM {{ source('mongo', 'b2b_core_offer_products_daily_snapshot') }}
    WHERE isDeleted IS NOT TRUE
),

     customer_offers AS (
    SELECT co._id AS customer_offer_id,
           co.csmrreqid AS customer_request_id,
           co.moderatorId AS customer_offer_owner_id,
           au.email AS customer_offer_owner_email,
           type.name AS customer_offer_type,
           status.name AS customer_offer_status,
           millis_to_ts_msk(co.ctms) AS customer_offer_created_ts,
           millis_to_ts_msk(co.utms) AS customer_offer_updated_ts
    FROM {{ source('mongo', 'b2b_core_customer_offers_daily_snapshot') }} AS co
    LEFT JOIN {{ ref('key_status') }} AS type ON type.key = 'offer.type' AND co.offerType = type.id
    LEFT JOIN {{ ref('key_status') }} AS status ON status.key = 'offer.status' AND co.status = status.id
    LEFT JOIN {{ source('mongo', 'b2b_core_admin_users_daily_snapshot') }} AS au ON co.moderatorId = au._id
    WHERE isDeleted IS NOT TRUE
),

     core_product AS (
    /* Если coreEmpty = True, то сущность продукта берется из offerProduts, иначе из orderProducts */
    /* Если coreEmpty = True и запись offerProduts удалена или отсутствует, то такой orderProduct удаляем */
    SELECT t1.procurement_order_id,
           t1.procurement_order_friendly_id,
           t1.deal_id,
           t1.deal_type,
           t1.country,
           t1.is_small_batch,
           t1.core_empty,
           CASE WHEN t1.core_empty = TRUE THEN t2.product_id ELSE t1.product_id END AS product_id,
           CASE WHEN t1.core_empty = TRUE THEN t2.product_name ELSE t1.product_name END AS product_name,
           t1.procurement_statuses,
           t1.current_psi_status_id_long,
           CASE WHEN t1.core_empty = TRUE THEN t2.manufacturer_id ELSE t1.manufacturer_id END AS manufacturer_id,
           t1.manufacturing_days,
           t1.production_range,
           t1.warehouse,
           t1.min_pickup_date,
           t1.merchant_order_id,
           t1.jpc_payment,
           t1.payment,
           t1.payment_count_plan,
           t1.prices,
           t1.product_roles,
           CASE WHEN t1.core_empty = TRUE THEN t2.offer_products_variants ELSE t1.procurement_order_variants END AS variants,
           CASE WHEN t1.core_empty = TRUE THEN t2.offer_product_created_ts ELSE t1.created_ts END AS created_ts,
           t1.updated_ts
    FROM procurement_orders AS t1
    LEFT JOIN offer_products AS t2 ON t1.procurement_order_id = t2.procurement_order_id
    WHERE t1.core_empty IS NOT TRUE
       OR (t1.core_empty = TRUE AND t2.procurement_order_id IS NOT NULL)
),

     pickup_orders AS (
    SELECT p.operationalProductId AS procurement_order_id,
           p._id AS pickup_order_id,
           friendlyId AS pickup_order_friendly_id,
           merchOrdId AS merchant_order_id,
           firstMileId AS first_mile_id,
           boxes,
           FILTER(boxes, x -> x.operationalProductId = p.operationalProductId) AS box,
           millis_to_ts_msk(ctms) AS creates_ts,
           millis_to_ts_msk(utms) AS updated_ts,
           to_date(cast(plannedDateV2 AS string), "yyyyMMdd") AS planned_date,
           to_date(cast(pickUpDateV2 AS string), "yyyyMMdd") AS pickup_date,
           to_date(cast(arrivedDateV2 AS string), "yyyyMMdd") AS arrived_date,
           to_date(cast(shippedDateV2 AS string), "yyyyMMdd") AS shipped_date,
           state AS statuses,
           current_status,
           current_status_ts,
           status_waiting_for_confirmation_ts,
           status_requested_ts,
           status_approved_ts,
           status_picked_up_ts,
           status_arrived_ts,
           status_shipped_ts,
           status_suspended_ts
    FROM (
        SELECT DISTINCT _id,
               b.operationalProductId AS operationalProductId
        FROM {{ source('mongo', 'b2b_core_pick_up_orders_v2_daily_snapshot') }}
        LATERAL VIEW explode(boxes) AS b
    ) AS p
    LEFT JOIN {{ source('mongo', 'b2b_core_pick_up_orders_v2_daily_snapshot') }} AS m ON p._id = m._id
    LEFT JOIN (
        SELECT _id,
               MAX_BY(status, status_time) AS current_status,
               MAX(status_time) AS current_status_ts,
               MIN(CASE WHEN status = 'WaitingForConfirmation' THEN status_time END) AS status_waiting_for_confirmation_ts,
               MIN(CASE WHEN status = 'Requested' THEN status_time END) AS status_requested_ts,
               MIN(CASE WHEN status = 'Approved' THEN status_time END) AS status_approved_ts,
               MIN(CASE WHEN status = 'PickedUp' THEN status_time END) AS status_picked_up_ts,
               MIN(CASE WHEN status = 'Arrived' THEN status_time END) AS status_arrived_ts,
               MIN(CASE WHEN status = 'Shipped' THEN status_time END) AS status_shipped_ts,
               MIN(CASE WHEN status = 'Suspended' THEN status_time END) AS status_suspended_ts
        FROM (
            SELECT _id,
                   CASE s.status
                       WHEN 2 THEN 'WaitingForConfirmation'
                       WHEN 5 THEN 'Requested'
                       WHEN 10 THEN 'Approved'
                       WHEN 13 THEN 'PickedUp'
                       WHEN 16 THEN 'Arrived'
                       WHEN 20 THEN 'Shipped'
                       WHEN 30 THEN 'Suspended'
                   END AS status,
                   millis_to_ts_msk(s.updatedTimeMs) AS status_time
            FROM {{ source('mongo', 'b2b_core_pick_up_orders_v2_daily_snapshot') }}
            LATERAL VIEW explode(state.statusHistory) AS s
        ) AS m
        GROUP BY 1
    ) AS s ON m._id = s._id
)


SELECT
    po.procurement_order_id,
    po.procurement_order_friendly_id,
    po.deal_id,
    co.customer_request_id,
    po.deal_type,
    po.country,
    po.is_small_batch,
    po.product_roles,
    pola.assignee_id,
    pola.assignee_email,
    pola.assignee_role,
    pola.assignee_ts,
    po.core_empty,
    po.product_id,
    po.product_name,
    po.prices,
    po.jpc_payment,
    fa.payment,
    po.payment_count_plan,
    pah.payment_count_fact,
    pah.received_payment_count,
    pah.overdue_payment_count,
    pah.payment_sum_usd,
    pah.payment_sum_cnh,
    pah.payment_sum_other,
    po.variants,
    millis_to_ts_msk(po.production_range.from) AS production_deadline_from,
    po.manufacturing_days,
    mo.manufacturing_days AS manufacturing_days_from_merchant_order,
    millis_to_ts_msk(po.production_range.deadline) AS production_deadline,
    millis_to_ts_msk(po.production_range.to) AS production_deadline_to,
    millis_to_ts_msk(warehouse.customsInfo.confirmationTime) AS confirmation_time,
    TIMESTAMP(warehouse.inspection.inspectionDate) AS warehouse_inspection_date,
    TIMESTAMP(warehouse.inspection.inspectionEta) AS warehouse_inspection_eta,
    TIMESTAMP(warehouse.merchantShipping.date) AS warehouse_merchant_shipping_date,
    warehouse.merchantShipping.deliveryDays AS warehouse_merchant_shipping_delivery_days,
    TIMESTAMP(warehouse.packingDate) AS warehouse_packing_date,
    TIMESTAMP(warehouse.receiving.receivingDate) AS warehouse_receiving_date,
    TIMESTAMP(warehouse.receiving.receivingEta) AS warehouse_receiving_eta,
    mo.client_order_id,
    po.merchant_order_id,
    mo.merchant_order_friendly_id,
    mo.merchant_id,
    mo.merchant_name,
    po.manufacturer_id,
    --mani.manufacturer_name
    op.customer_offer_id,
    co.customer_offer_owner_id,
    co.customer_offer_owner_email,
    co.customer_offer_type,
    co.customer_offer_status,
    coalesce(po.created_ts, pch.first_status_ts) AS created_ts,
    greatest(po.updated_ts, coalesce(po.created_ts, pch.first_status_ts)) AS updated_ts,
    po.procurement_statuses,
    pch.current_status,
    pch.current_sub_status,
    pch.current_status_ts,
    pch.sub_status_forming_order_unassigned_ts,
    pch.sub_status_filling_in_information_ts,
    pch.sub_status_confirmed_by_procurement_ts,
    pch.sub_status_waiting_for_payment_ts,
    pch.sub_status_merchant_preparing_order_ts,
    pch.sub_status_merchant_shipped_the_goods_ts,
    pch.sub_status_issues_ts,
    pch.sub_status_preparing_order_ts,
    pch.sub_status_awaiting_for_signing_ts,
    pch.sub_status_merchant_signed_ts,
    pch.sub_status_client_payment_received_ts,
    pch.sub_status_advance_payment_requested_ts,
    pch.sub_status_advance_payment_in_progress_ts,
    pch.sub_status_manufacturing_ts,
    pch.sub_status_psi_being_conducted_ts,
    pch.sub_status_psi_waiting_for_confirmation_ts,
    pch.sub_status_psi_problems_are_to_be_fixed_ts,
    pch.sub_status_psi_results_accepted_ts,
    pch.sub_status_psi_approved_manually_ts,
    pch.sub_status_packing_and_labeling_ts,
    pch.sub_status_ready_for_shipment_ts,
    pch.sub_status_shipped_by_3pl_ts,
    pch.sub_status_final_payment_requested_ts,
    pch.sub_status_final_payment_in_progress_ts,
    pch.sub_status_final_payment_acquired_ts,
    pch.sub_status_pick_up_payment_picked_up_ts,
    pch.sub_status_completed_ts,
    pch.sub_status_cancelled_ts,
    ph.current_psi_status,
    ph.current_psi_status_ts,
    ph.psi_waiting_ts,
    ph.psi_running_ts AS psi_being_conducted_ts,
    ph.inspection_ts,
    ph.psi_ready_ts AS psi_waiting_for_confirmation_ts,
    ph.solution,
    ph.problem_quality,
    ph.problem_customs,
    ph.problem_logistics,
    ph.problem_discrepancy,
    ph.problem_requirements,
    ph.problem_other,
    ph.problem_comment,
    ph.psi_fail_ts AS psi_problems_are_to_be_fixed_ts,
    ph.problem_client_quality,
    ph.problem_client_customs,
    ph.problem_client_logistics,
    ph.problem_client_discrepancy,
    ph.problem_client_requirements,
    ph.problem_client_other,
    ph.problem_client_comment,
    ph.psi_success_ts AS psi_results_accepted_ts,
    pio.pickup_order_id,
    pio.pickup_order_friendly_id,
    pio.box AS pickup_order_box,
    pio.boxes AS pickup_order_boxes,
    pio.creates_ts AS pickup_order_created_ts,
    pio.updated_ts AS pickup_order_updated_ts,
    po.min_pickup_date AS procurement_order_planned_pickup_date,
    pio.planned_date AS pickup_order_planned_date,
    pio.pickup_date AS pickup_order_pickup_date,
    pio.arrived_date AS pickup_order_arrived_date,
    pio.shipped_date AS pickup_order_shipped_date,
    pio.statuses AS pickup_order_statuses,
    pio.current_status AS pickup_order_current_status,
    pio.current_status_ts AS pickup_order_current_status_ts,
    pio.status_waiting_for_confirmation_ts AS pickup_order_status_waiting_for_confirmation_ts,
    pio.status_requested_ts AS pickup_order_status_requested_ts,
    pio.status_approved_ts AS pickup_order_status_approved_ts,
    pio.status_picked_up_ts AS pickup_order_status_picked_up_ts,
    pio.status_arrived_ts AS pickup_order_status_arrived_ts,
    pio.status_shipped_ts AS pickup_order_status_shipped_ts,
    pio.status_suspended_ts AS pickup_order_status_suspended_ts,
    CASE
        /* Для крупного опта в Бразилии */
        WHEN po.country = 'BR' AND po.is_small_batch = 0 THEN
            CASE
                /* Убираем отмененные заказы, если отмена произошла до оплаты клиентом */
                WHEN current_sub_status = 'cancelled'
                 AND (
                      sub_status_client_payment_received_ts IS NULL OR
                      sub_status_cancelled_ts < sub_status_client_payment_received_ts
                 ) THEN 0
                /* Оставляем реальные заказы из переданного списка */
                WHEN po.procurement_order_friendly_id IN (
                    'ZVM3D', 'KLG6J', 'EWR3D', 'YGQYK', 'PZG7Z', '525YP', 'VL6VG', 'MWGXM', 'Q3ZX2', '525V5',
                    '3PMQQ', 'PZGNE', 'YGQN2', 'N3GP6', '525NG', 'KLGQ5', 'ZVME7', 'RVGLY', 'XJ5WE', 'JDGEK',
                    'WL2QX', '5273K', 'XJDVE', 'VLMGL', 'PZEJV', 'L2XD6', 'YGQP8', '3PM8V', 'G8GXY', '86DNK',
                    '2G8KD', 'XJ5YX', '86DP6', 'N3L8P', 'XJ52P', 'JD5YM', 'VLMPK', '732KQ', 'L2YKW', 'YGZZR',
                    'PZE2Q', '86DXY', 'N3L7J', 'RV6QQ', '5277Q', 'MW33Z', 'DJ66R', 'EW228', '732XQ', '2GRYR',
                    'RV6DK', 'MWZ2R', '2G8V3', 'Q3YLD', '73KW2', '638R8', '2G8WD', 'DJLE2', 'Q3DLJ', 'VLP8G',
                    'MWEJQ', 'MWEPM', 'Q3DJ2', 'G8EGM', 'VLE6Y', '638PN', 'XJ6D3', '2G8Q3', 'VLEPG', 'EWVRY',
                    '528LP', 'PZP2Z', 'MWZLM', 'Q3YK2', 'JDNDJ', 'G8PRY', 'KLJZJ', 'XJNXE', 'N3NMG', 'WLN3X',
                    'ZVN8D', '86YJ7', 'RVN86', 'XJ3XY', 'N378Q', '86XP8', 'WLY3Q', '86YR6', 'Q3K2M', 'JDNRJ',
                    'Q3KG2', '73VDP', '2G6XP', '738J5', '63WNN', 'EWZMG', '2GL23', 'Q3DVL', 'DJYXV', 'Q32WM',
                    'YG3XQ', '52JLE', 'YGRR5', '3P33P', 'MWRER', 'Q3RDD', 'L288L', 'VLRP5', 'PZRRJ', 'DJZN2',
                    'XJYQQ', 'JDWKJ', 'G8J5V', 'JDEDM'
                ) THEN 1
                /* Оставляем все заказы в админке с 1 марта 2025 */
                WHEN DATE(coalesce(po.created_ts, pch.first_status_ts)) >= '2025-03-01' THEN 1
                ELSE 0
            END
        /* Для мелкого опта в Казахстане */
        WHEN po.country = 'KZ' AND po.is_small_batch = 1 THEN
            CASE
                /* Оставляем все заказы в админке с 1 апреля 2025 */
                WHEN DATE(coalesce(po.created_ts, pch.first_status_ts)) >= '2025-04-01' THEN 1
                ELSE 0
            END
        ELSE 1
    END AS is_for_purchasing_and_production_report
FROM core_product AS po
LEFT JOIN filtered_payments AS fa ON po.procurement_order_id = fa.procurement_order_id
LEFT JOIN payments_history AS pah ON po.procurement_order_id = pah.procurement_order_id
LEFT JOIN procurement_orders_last_assignee AS pola ON po.procurement_order_id = pola.procurement_order_id
LEFT JOIN merchant_orders AS mo ON po.merchant_order_id = mo.merchant_order_id
LEFT JOIN offer_products AS op ON po.procurement_order_id = op.procurement_order_id
LEFT JOIN customer_offers AS co ON op.customer_offer_id = co.customer_offer_id
LEFT JOIN procurement_statuses_history AS pch ON po.procurement_order_id = pch.procurement_order_id
LEFT JOIN psi_history AS ph ON po.current_psi_status_id_long = ph.current_psi_status_id_long
LEFT JOIN pickup_orders AS pio ON po.procurement_order_id = pio.procurement_order_id
/*
LEFT JOIN (
    SELECT '670fbb9915432be923fd693b' AS procurement_order_id
    UNION ALL
    SELECT '670fbb9915432be923fd693b' AS procurement_order_id
) AS test ON po.procurement_order_id = test.procurement_order_id
*/
