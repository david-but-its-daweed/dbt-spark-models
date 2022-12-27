{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

select
'XN2QX_XE6J3' AS friendly_id,
'2022-7-15' AS advance_payment_requested,
'2022-7-19' AS advance_payment_in_progress,
'2022-7-19' AS advance_payment_acquired,
5 AS manufacturing_and_qc_in_progress,
'2022-7-20' AS ready_for_psi,
'2022-7-20' AS ready_for_psi,
'2022-7-23' AS psi_ready,
'2022-7-23' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-8-1' AS remaining_payment_in_progress,
'2022-8-1' AS remaining_payment_acquired,
'2022-8-3' AS shipped
union all
select
'XN2QX_XZZVX' AS friendly_id,
'2022-7-15' AS advance_payment_requested,
'2022-7-19' AS advance_payment_in_progress,
'2022-7-21' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-7-29' AS ready_for_psi,
'2022-7-29' AS ready_for_psi,
'2022-7-29' AS psi_ready,
'2022-8-1' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-8-1' AS remaining_payment_in_progress,
'2022-8-2' AS remaining_payment_acquired,
'2022-8-3' AS shipped
union all
select
'3W27G' AS friendly_id,
'2022-7-7' AS advance_payment_requested,
'2022-7-11' AS advance_payment_in_progress,
'2022-7-12' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-7-31' AS ready_for_psi,
'2022-7-31' AS ready_for_psi,
'2022-8-3' AS psi_ready,
'2022-8-8' AS client_gave_feedback,
'2022-8-8' AS problems_are_fixed,
'2022-8-10' AS remaining_payment_in_progress,
'2022-8-11' AS remaining_payment_acquired,
'2022-8-15' AS shipped
union all
select
'XJKWQ' AS friendly_id,
'2022-7-28' AS advance_payment_requested,
'2022-7-28' AS advance_payment_in_progress,
'2022-07-29' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-8-8' AS ready_for_psi,
'2022-8-8' AS ready_for_psi,
'2022-8-9' AS psi_ready,
'2022-8-11' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-8-11' AS remaining_payment_in_progress,
'2022-8-15' AS remaining_payment_acquired,
'2022-8-16' AS shipped
union all
select
'XGNWM' AS friendly_id,
'2022-8-12' AS advance_payment_requested,
'2022-8-8' AS advance_payment_in_progress,
'2022-8-10' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-8-14' AS ready_for_psi,
'2022-8-14' AS ready_for_psi,
'2022-8-15' AS psi_ready,
'2022-8-16' AS client_gave_feedback,
'2022-8-16' AS problems_are_fixed,
'2022-8-18' AS remaining_payment_in_progress,
'2022-8-19' AS remaining_payment_acquired,
'2022-8-21' AS shipped
union all
select
'XNW7Q' AS friendly_id,
'2022-8-25' AS advance_payment_requested,
'2022-8-5' AS advance_payment_in_progress,
'2022-8-10' AS advance_payment_acquired,
17 AS manufacturing_and_qc_in_progress,
'2022-8-29' AS ready_for_psi,
'2022-8-29' AS ready_for_psi,
'2022-8-29' AS psi_ready,
'2022-8-29' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-8-30' AS remaining_payment_in_progress,
'2022-8-31' AS remaining_payment_acquired,
'2022-9-1' AS shipped
union all
select
'KRE4P' AS friendly_id,
'2022-8-26' AS advance_payment_requested,
'2022-8-22' AS advance_payment_in_progress,
'2022-8-23' AS advance_payment_acquired,
30 AS manufacturing_and_qc_in_progress,
'2022-9-1' AS ready_for_psi,
'2022-9-1' AS ready_for_psi,
'2022-9-2' AS psi_ready,
'2022-9-5' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'' AS remaining_payment_acquired,
'' AS remaining_payment_acquired,
'2022-9-7' AS shipped
union all
select
'KWW72_YLLPW' AS friendly_id,
'2022-8-25' AS advance_payment_requested,
'2022-8-25' AS advance_payment_in_progress,
'2022-8-25' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-8-31' AS ready_for_psi,
'2022-8-31' AS ready_for_psi,
'2022-8-31' AS psi_ready,
'2022-9-1' AS client_gave_feedback,
'2022-9-1' AS problems_are_fixed,
'2022-9-6' AS remaining_payment_in_progress,
'2022-9-6' AS remaining_payment_acquired,
'2022-9-9' AS shipped
union all
select
'KWW72_YNN2L' AS friendly_id,
'2022-8-25' AS advance_payment_requested,
'2022-8-25' AS advance_payment_in_progress,
'2022-8-25' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-9-1' AS ready_for_psi,
'2022-9-1' AS ready_for_psi,
'2022-9-1' AS psi_ready,
'2022-9-1' AS client_gave_feedback,
'2022-9-5' AS problems_are_fixed,
'2022-9-6' AS remaining_payment_in_progress,
'2022-9-6' AS remaining_payment_acquired,
'2022-9-9' AS shipped
union all
select
'XEJK3' AS friendly_id,
'2022-6-20' AS advance_payment_requested,
'2022-6-21' AS advance_payment_in_progress,
'2022-6-22' AS advance_payment_acquired,
60 AS manufacturing_and_qc_in_progress,
'2022-8-16' AS ready_for_psi,
'2022-8-16' AS ready_for_psi,
'2022-8-19' AS psi_ready,
'2022-8-22' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-8-22' AS remaining_payment_in_progress,
'2022-8-23' AS remaining_payment_acquired,
'2022-9-10' AS shipped
union all
select
'X5RV3' AS friendly_id,
'2022-6-30' AS advance_payment_requested,
'2022-7-1' AS advance_payment_in_progress,
'2022-7-3' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-8-8' AS ready_for_psi,
'2022-8-8' AS ready_for_psi,
'2022-8-8' AS psi_ready,
'2022-8-11' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-8-11' AS remaining_payment_in_progress,
'2022-8-13' AS remaining_payment_acquired,
'2022-9-10' AS shipped
union all
select
'J39RY' AS friendly_id,
'2022-8-31' AS advance_payment_requested,
'2022-8-31' AS advance_payment_in_progress,
'2022-8-31' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-9-10' AS ready_for_psi,
'2022-9-10' AS ready_for_psi,
'2022-9-12' AS psi_ready,
'2022-9-14' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-9-14' AS remaining_payment_in_progress,
'2022-9-14' AS remaining_payment_acquired,
'2022-9-15' AS shipped
union all
select
'XQMDY' AS friendly_id,
'2022-9-7' AS advance_payment_requested,
'2022-8-30' AS advance_payment_in_progress,
'2022-9-1' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-9-6' AS ready_for_psi,
'2022-9-6' AS ready_for_psi,
'2022-9-7' AS psi_ready,
'2022-9-7' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-9-12' AS remaining_payment_in_progress,
'2022-9-14' AS remaining_payment_acquired,
'2022-9-15' AS shipped
union all
select
'KE7XR' AS friendly_id,
'2022-8-22' AS advance_payment_requested,
'2022-8-22' AS advance_payment_in_progress,
'2022-8-23' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-9-4' AS ready_for_psi,
'2022-9-4' AS ready_for_psi,
'2022-9-5' AS psi_ready,
'2022-9-7' AS client_gave_feedback,
'2022-9-13' AS problems_are_fixed,
'2022-9-14' AS remaining_payment_in_progress,
'2022-9-14' AS remaining_payment_acquired,
'2022-9-16' AS shipped
union all
select
'3D4GL' AS friendly_id,
'2022-8-29' AS advance_payment_requested,
'2022-8-30' AS advance_payment_in_progress,
'2022-8-31' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-9-6' AS ready_for_psi,
'2022-9-6' AS ready_for_psi,
'2022-9-6' AS psi_ready,
'2022-9-6' AS client_gave_feedback,
'2022-9-13' AS problems_are_fixed,
'2022-9-15' AS remaining_payment_in_progress,
'2022-9-16' AS remaining_payment_acquired,
'2022-9-17' AS shipped
union all
select
'J2QYV' AS friendly_id,
'2022-9-15' AS advance_payment_requested,
'2022-9-15' AS advance_payment_in_progress,
'2022-9-16' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-9-16' AS ready_for_psi,
'2022-9-16' AS ready_for_psi,
'2022-9-20' AS psi_ready,
'2022-9-20' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-5' AS remaining_payment_in_progress,
'2022-10-6' AS remaining_payment_acquired,
'2022-9-21' AS shipped
union all
select
'X49KL' AS friendly_id,
'2022-8-26' AS advance_payment_requested,
'2022-8-26' AS advance_payment_in_progress,
'2022-8-26' AS advance_payment_acquired,
30 AS manufacturing_and_qc_in_progress,
'2022-9-16' AS ready_for_psi,
'2022-9-16' AS ready_for_psi,
'2022-9-16' AS psi_ready,
'2022-9-19' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-9-19' AS remaining_payment_in_progress,
'2022-9-19' AS remaining_payment_acquired,
'2022-9-21' AS shipped
union all
select
'KMQ98' AS friendly_id,
'2022-8-26' AS advance_payment_requested,
'2022-8-30' AS advance_payment_in_progress,
'2022-9-1' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-9-19' AS ready_for_psi,
'2022-9-19' AS ready_for_psi,
'2022-9-20' AS psi_ready,
'2022-9-20' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-9-21' AS remaining_payment_in_progress,
'2022-9-22' AS remaining_payment_acquired,
'2022-9-23' AS shipped
union all
select
'KGLEL' AS friendly_id,
'2022-8-22' AS advance_payment_requested,
'2022-8-22' AS advance_payment_in_progress,
'2022-8-24' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-9-19' AS ready_for_psi,
'2022-9-19' AS ready_for_psi,
'2022-9-19' AS psi_ready,
'2022-9-20' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-9-22' AS remaining_payment_in_progress,
'2022-9-26' AS remaining_payment_acquired,
'2022-9-27' AS shipped
union all
select
'3KR5X' AS friendly_id,
'2022-8-29' AS advance_payment_requested,
'2022-9-6' AS advance_payment_in_progress,
'2022-9-7' AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
'2022-8-27' AS ready_for_psi,
'2022-8-27' AS ready_for_psi,
'2022-8-28' AS psi_ready,
'2022-8-28' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-9-30' AS remaining_payment_in_progress,
'2022-9-30' AS remaining_payment_acquired,
'2022-10-8' AS shipped
union all
select
'J4QE6' AS friendly_id,
'2022-9-20' AS advance_payment_requested,
'2022-9-20' AS advance_payment_in_progress,
'2022-9-21' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-10-10' AS ready_for_psi,
'2022-10-10' AS ready_for_psi,
'2022-10-10' AS psi_ready,
'2022-10-10' AS client_gave_feedback,
'2022-10-18' AS problems_are_fixed,
'2022-10-18' AS remaining_payment_in_progress,
'2022-10-19' AS remaining_payment_acquired,
'2022-10-21' AS shipped
union all
select
'J2Q29' AS friendly_id,
'2022-9-20' AS advance_payment_requested,
'2022-9-20' AS advance_payment_in_progress,
'2022-9-22' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-10-17' AS ready_for_psi,
'2022-10-17' AS ready_for_psi,
'2022-10-17' AS psi_ready,
'2022-10-18' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-19' AS remaining_payment_in_progress,
'2022-10-21' AS remaining_payment_acquired,
'2022-10-22' AS shipped
union all
select
'K6DZX' AS friendly_id,
'2022-9-22' AS advance_payment_requested,
'2022-9-26' AS advance_payment_in_progress,
'2022-9-28' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-10-10' AS ready_for_psi,
'2022-10-10' AS ready_for_psi,
'2022-10-11' AS psi_ready,
'2022-10-11' AS client_gave_feedback,
'2022-10-12' AS problems_are_fixed,
'2022-10-13' AS remaining_payment_in_progress,
'2022-10-14' AS remaining_payment_acquired,
'2022-10-23' AS shipped
union all
select
'JV349' AS friendly_id,
'2022-9-29' AS advance_payment_requested,
'2022-9-30' AS advance_payment_in_progress,
'2022-10-1' AS advance_payment_acquired,
22 AS manufacturing_and_qc_in_progress,
'2022-19-10' AS ready_for_psi,
'2022-19-10' AS ready_for_psi,
'2022-10-20' AS psi_ready,
'2022-10-20' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-20' AS remaining_payment_in_progress,
'2022-10-24' AS remaining_payment_acquired,
'2022-10-25' AS shipped
union all
select
'KDX4M_Y4VE9' AS friendly_id,
'2022-10-5' AS advance_payment_requested,
'2022-10-6' AS advance_payment_in_progress,
'2022-10-6' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-10-22' AS ready_for_psi,
'2022-10-22' AS ready_for_psi,
'2022-10-24' AS psi_ready,
'2022-10-24' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-24' AS remaining_payment_in_progress,
'2022-10-25' AS remaining_payment_acquired,
'2022-10-26' AS shipped
union all
select
'KDX4M_G57QX' AS friendly_id,
'2022-10-5' AS advance_payment_requested,
'2022-10-6' AS advance_payment_in_progress,
'2022-10-6' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-10-12' AS ready_for_psi,
'2022-10-12' AS ready_for_psi,
'2022-10-14' AS psi_ready,
'2022-10-17' AS client_gave_feedback,
'2022-10-19' AS problems_are_fixed,
'2022-10-19' AS remaining_payment_in_progress,
'2022-10-19' AS remaining_payment_acquired,
'2022-10-26' AS shipped
union all
select
'JZYL9' AS friendly_id,
'2022-8-29' AS advance_payment_requested,
'2022-9-6' AS advance_payment_in_progress,
'2022-9-6' AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
'2022-9-27' AS ready_for_psi,
'2022-9-27' AS ready_for_psi,
'2022-10-19' AS psi_ready,
'2022-10-20' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-20' AS remaining_payment_in_progress,
'2022-10-20' AS remaining_payment_acquired,
'2022-10-26' AS shipped
union all
select
'K5VPE' AS friendly_id,
'2022-10-3' AS advance_payment_requested,
'2022-10-4' AS advance_payment_in_progress,
'2022-10-6' AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
'2022-10-20' AS ready_for_psi,
'2022-10-20' AS ready_for_psi,
'2022-10-21' AS psi_ready,
'2022-10-24' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-24' AS remaining_payment_in_progress,
'2022-10-25' AS remaining_payment_acquired,
'2022-10-27' AS shipped
union all
select
'K5V88' AS friendly_id,
'2022-10-07' AS advance_payment_requested,
'2022-10-13' AS advance_payment_in_progress,
'2022-10-14' AS advance_payment_acquired,
7 AS manufacturing_and_qc_in_progress,
'2022-10-24' AS ready_for_psi,
'2022-10-24' AS ready_for_psi,
'2022-10-24' AS psi_ready,
'2022-10-25' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-26' AS remaining_payment_in_progress,
'2022-10-27' AS remaining_payment_acquired,
'2022-10-28' AS shipped
union all
select
'KGRDV' AS friendly_id,
'2022-10-5' AS advance_payment_requested,
'2022-10-6' AS advance_payment_in_progress,
'2022-10-11' AS advance_payment_acquired,
11 AS manufacturing_and_qc_in_progress,
'2022-10-24' AS ready_for_psi,
'2022-10-24' AS ready_for_psi,
'2022-10-24' AS psi_ready,
'2022-10-27' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-10-27' AS remaining_payment_in_progress,
'2022-10-31' AS remaining_payment_acquired,
'2022-10-31' AS shipped
union all
select
'JQZ5M' AS friendly_id,
'2022-10-14' AS advance_payment_requested,
'2022-10-17' AS advance_payment_in_progress,
'2022-10-19' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-10-29' AS ready_for_psi,
'2022-10-29' AS ready_for_psi,
'2022-11-3' AS psi_ready,
'2022-11-3' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-11-7' AS remaining_payment_in_progress,
'2022-11-9' AS remaining_payment_acquired,
'2022-11-10' AS shipped
union all
select
'JV3XE' AS friendly_id,
'2022-10-19' AS advance_payment_requested,
'2022-10-20' AS advance_payment_in_progress,
'2022-10-21' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-11-3' AS ready_for_psi,
'2022-11-3' AS ready_for_psi,
'2022-11-3' AS psi_ready,
'2022-11-8' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-11-8' AS remaining_payment_in_progress,
'2022-11-9' AS remaining_payment_acquired,
'2022-11-10' AS shipped
union all
select
'3DDLN' AS friendly_id,
'2022-10-3' AS advance_payment_requested,
'2022-9-30' AS advance_payment_in_progress,
'2022-10-10' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-10-20' AS ready_for_psi,
'2022-10-20' AS ready_for_psi,
'2022-10-20' AS psi_ready,
'2022-10-21' AS client_gave_feedback,
'2022-11-7' AS problems_are_fixed,
'2022-10-20' AS remaining_payment_in_progress,
'2022-10-21' AS remaining_payment_acquired,
'2022-11-12' AS shipped
union all
select
'J9979' AS friendly_id,
'2022-10-31' AS advance_payment_requested,
'2022-10-28' AS advance_payment_in_progress,
'2022-10-31' AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
'2022-11-10' AS ready_for_psi,
'2022-11-10' AS ready_for_psi,
'2022-11-10' AS psi_ready,
'2022-11-14' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-11-14' AS remaining_payment_in_progress,
'2022-11-16' AS remaining_payment_acquired,
'2022-11-18' AS shipped
union all
select
'KLGNE' AS friendly_id,
'2022-10-7' AS advance_payment_requested,
'2022-10-10' AS advance_payment_in_progress,
'2022-10-11' AS advance_payment_acquired,
30 AS manufacturing_and_qc_in_progress,
'2022-11-7' AS ready_for_psi,
'2022-11-7' AS ready_for_psi,
'2022-11-7' AS psi_ready,
'2022-11-8' AS client_gave_feedback,
'2022-11-9' AS problems_are_fixed,
'2022-11-10' AS remaining_payment_in_progress,
'2022-11-12' AS remaining_payment_acquired,
'2022-11-22' AS shipped
union all
select
'KLZWL' AS friendly_id,
'2022-10-31' AS advance_payment_requested,
'2022-10-28' AS advance_payment_in_progress,
'2022-10-30' AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
'2022-11-15' AS ready_for_psi,
'2022-11-15' AS ready_for_psi,
'2022-11-16' AS psi_ready,
'2022-11-17' AS client_gave_feedback,
'2022-11-21' AS problems_are_fixed,
'2022-11-23' AS remaining_payment_in_progress,
'2022-11-24' AS remaining_payment_acquired,
'2022-11-25' AS shipped
union all
select
'JYDE2' AS friendly_id,
'2022-10-5' AS advance_payment_requested,
'2022-10-6' AS advance_payment_in_progress,
'2022-10-11' AS advance_payment_acquired,
16 AS manufacturing_and_qc_in_progress,
'2022-10-27' AS ready_for_psi,
'2022-10-27' AS ready_for_psi,
'2022-11-21' AS psi_ready,
'2022-11-23' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-11-23' AS remaining_payment_in_progress,
'2022-11-24' AS remaining_payment_acquired,
'2022-11-26' AS shipped
union all
select
'JNEVD' AS friendly_id,
'2022-10-31' AS advance_payment_requested,
'2022-10-28' AS advance_payment_in_progress,
'2022-10-29' AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
'2022-11-21' AS ready_for_psi,
'2022-11-21' AS ready_for_psi,
'2022-11-21' AS psi_ready,
'2022-11-22' AS client_gave_feedback,
'' AS remaining_payment_in_progress,
'2022-11-23' AS remaining_payment_in_progress,
'2022-11-24' AS remaining_payment_acquired,
'2022-11-30' AS shipped
