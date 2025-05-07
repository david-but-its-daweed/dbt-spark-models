{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@mkirusha',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH base_deals
AS (
	SELECT deal_id
		,deal_status
		,deal_status_group
		,deal_created_date
		,deal_type
		,country
	FROM {{ ref('fact_deals_with_requests') }} 
	)
	,c_s
AS (
	SELECT deal_status AS c_deal_status
	FROM base_deals
	WHERE deal_status_group = 'Cancelled'
	GROUP BY 1
	)
SELECT deal_id
	,deal_status
	,deal_status_group
	,deal_created_date
	,deal_type
	,STATUS
	,CASE 
		WHEN STATUS = 'New'
			THEN 'a_New'
		WHEN STATUS = 'RequestRetrieval'
			THEN 'b_RequestRetrieval'
		WHEN STATUS = 'PreparingSalesQuote'
			THEN 'c_PreparingSalesQuote'
		WHEN STATUS = 'FormingOrder'
			THEN 'd_FormingOrder'
		WHEN STATUS = 'ProcurementConfirmation'
			THEN 'e_ProcurementConfirmation'
		WHEN STATUS = 'DocumentsSent'
			THEN 'f_DocumentsSent'
		WHEN STATUS = 'SignedDocumentsReceived'
			THEN 'g_SignedDocumentsReceived'
		WHEN STATUS = 'ClientToBrokerPaymentSent'
			THEN 'h_ClientToBrokerPaymentSent'
		WHEN STATUS = 'JoomPaymentReceived'
			THEN 'i_JoomPaymentReceived'
		WHEN STATUS = 'PaymentToMerchant'
			THEN 'j_PaymentToMerchant'
		WHEN STATUS = 'ManufacturingAndDelivery'
			THEN 'k_ManufacturingAndDelivery'
		WHEN STATUS = 'Delivering'
			THEN 'l_Delivering'
		WHEN STATUS = 'DealCompleted'
			THEN 'm_DealCompleted'
		END AS status_name
	,CASE 
		WHEN STATUS = 'New'
			THEN 'a_New'
		WHEN STATUS = 'PreparingSalesQuote'
			THEN 'b_PreparingSalesQuote'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'JpSigningDocuments'
				)
			OR (
				type = 'CloseTheDealBrazil'
				AND STATUS = 'DocumentsSent'
				)
			THEN 'c_JpSigningDocuments'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'ClientSigningDocuments'
				)
			OR (
				type = 'CloseTheDealBrazil'
				AND STATUS = 'SignedDocumentsReceived'
				)
			THEN 'd_ClientSigningDocuments'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'WaitingClientsPayment'
				)
			OR (
				type = 'CloseTheDealBrazil'
				AND STATUS = 'ClientToBrokerPaymentSent'
				)
			THEN 'e_WaitingClientsPayment'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'FinancePaymentConfirmation'
				)
			OR (
				type = 'CloseTheDealBrazil'
				AND STATUS = 'JoomPaymentReceived'
				)
			THEN 'f_FinancePaymentConfirmation'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'ProcurementConfirmation'
				)
			OR (
				type = 'CloseTheDealBrazil'
				AND STATUS = 'PaymentToMerchant'
				)
			THEN 'g_ProcurementConfirmation'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'ProcessedByProcurement'
				)
			THEN 'h_ProcessedByProcurement'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'PaymentToMerchant'
				)
			THEN 'i_PaymentToMerchant'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'Manufacturing'
				)
			OR (
				type = 'CloseTheDealBrazil'
				AND STATUS = 'ManufacturingAndDelivery'
				)
			THEN 'j_Manufacturing'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				OR type = 'CloseTheDealBrazil'
				)
			AND STATUS = 'Delivering'
			THEN 'k_Delivering'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'InvoiceIssuance'
				)
			THEN 'l_InvoiceIssuance'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'InvoiceSigningAndPayment'
				)
			THEN 'm_InvoiceSigningAndPayment'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				AND STATUS = 'LastMile'
				)
			THEN 'n_LastMile'
		WHEN (
				type = 'CloseTheDealBrazilSmallBatch'
				OR type = 'CloseTheDealBrazil'
				)
			AND STATUS = 'DealCompleted'
			THEN 'o_DealCompleted'
		END AS status_name_small_deal
	,type AS status_type
	,CASE 
		WHEN STATUS = c_deal_status
			THEN 1
		ELSE 0
		END AS status_cancelled
	,status_int
	,lead(STATUS) OVER (
		PARTITION BY entity_id ORDER BY event_ts_msk
		) AS next_status
	,CASE 
		WHEN lead(c_deal_status) OVER (
				PARTITION BY entity_id ORDER BY event_ts_msk
				) IS NOT NULL
			THEN 1
		ELSE 0
		END AS next_status_cancelled
	,event_ts_msk
	,created_ts_msk
	,row_number() OVER (
		PARTITION BY entity_id ORDER BY event_ts_msk
		) status_number
	,(UNIX_TIMESTAMP(LEAD(event_ts_msk) OVER (
    			PARTITION BY entity_id ORDER BY event_ts_msk)) - UNIX_TIMESTAMP(event_ts_msk)) / 3600.0 AS status_time_hours,
 	 DATEDIFF(
    		LEAD(event_ts_msk) OVER (
     		 PARTITION BY entity_id ORDER BY event_ts_msk), event_ts_msk) AS status_time_days
	,lead(event_ts_msk) OVER (
		PARTITION BY entity_id ORDER BY event_ts_msk
		) AS next_status_ts
	,CASE 
		WHEN row_number() OVER (
				PARTITION BY entity_id ORDER BY event_ts_msk DESC
				) = 1
			THEN true
		ELSE false
		END is_actual_status
FROM {{ ref('fact_issues_statuses') }}  fis
JOIN base_deals ON fis.entity_id = base_deals.deal_id
LEFT JOIN c_s ON fis.STATUS = c_s.c_deal_status
WHERE country = 'BR'
	AND type IN (
		'CloseTheDealBrazilSmallBatch'
		,'CloseTheDealBrazil'
		)
