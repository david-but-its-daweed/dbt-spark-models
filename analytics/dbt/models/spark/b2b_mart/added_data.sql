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
date(15,7,2022) AS advance_payment_requested,
date(19,7,2022) AS advance_payment_in_progress,
date(19,7,2022) AS advance_payment_acquired,
5 AS manufacturing_and_qc_in_progress,
date(20,7,2022) AS ready_for_psi,
date(20,7,2022) AS ready_for_psi,
date(23,7,2022) AS psi_ready,
date(23,7,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(1,8,2022) AS remaining_payment_in_progress,
date(1,8,2022) AS remaining_payment_acquired,
date(3,8,2022) AS shipped
union all
select
'XN2QX_XZZVX' AS friendly_id,
date(15,7,2022) AS advance_payment_requested,
date(19,7,2022) AS advance_payment_in_progress,
date(21,7,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(29,7,2022) AS ready_for_psi,
date(29,7,2022) AS ready_for_psi,
date(29,7,2022) AS psi_ready,
date(1,8,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(1,8,2022) AS remaining_payment_in_progress,
date(2,8,2022) AS remaining_payment_acquired,
date(3,8,2022) AS shipped
union all
select
'3W27G' AS friendly_id,
date(7,7,2022) AS advance_payment_requested,
date(11,7,2022) AS advance_payment_in_progress,
date(12,7,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(31,7,2022) AS ready_for_psi,
date(31,7,2022) AS ready_for_psi,
date(3,8,2022) AS psi_ready,
date(8,8,2022) AS client_gave_feedback,
date(8,8,2022) AS problems_are_fixed,
date(10,8,2022) AS remaining_payment_in_progress,
date(11,8,2022) AS remaining_payment_acquired,
date(15,8,2022) AS shipped
union all
select
'XJKWQ' AS friendly_id,
date(28,7,2022) AS advance_payment_requested,
date(28,7,2022) AS advance_payment_in_progress,
date(29,07,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(8,8,2022) AS ready_for_psi,
date(8,8,2022) AS ready_for_psi,
date(9,8,2022) AS psi_ready,
date(11,8,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(11,8,2022) AS remaining_payment_in_progress,
date(15,8,2022) AS remaining_payment_acquired,
date(16,8,2022) AS shipped
union all
select
'XGNWM' AS friendly_id,
date(12,8,2022) AS advance_payment_requested,
date(8,8,2022) AS advance_payment_in_progress,
date(10,8,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(14,8,2022) AS ready_for_psi,
date(14,8,2022) AS ready_for_psi,
date(15,8,2022) AS psi_ready,
date(16,8,2022) AS client_gave_feedback,
date(16,8,2022) AS problems_are_fixed,
date(18,8,2022) AS remaining_payment_in_progress,
date(19,8,2022) AS remaining_payment_acquired,
date(21,8,2022) AS shipped
union all
select
'XNW7Q' AS friendly_id,
date(25,8,2022) AS advance_payment_requested,
date(5,8,2022) AS advance_payment_in_progress,
date(10,8,2022) AS advance_payment_acquired,
17 AS manufacturing_and_qc_in_progress,
date(29,8,2022) AS ready_for_psi,
date(29,8,2022) AS ready_for_psi,
date(29,8,2022) AS psi_ready,
date(29,8,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(30,8,2022) AS remaining_payment_in_progress,
date(31,8,2022) AS remaining_payment_acquired,
date(1,9,2022) AS shipped
union all
select
'KRE4P' AS friendly_id,
date(26,8,2022) AS advance_payment_requested,
date(22,8,2022) AS advance_payment_in_progress,
date(23,8,2022) AS advance_payment_acquired,
30 AS manufacturing_and_qc_in_progress,
date(1,9,2022) AS ready_for_psi,
date(1,9,2022) AS ready_for_psi,
date(2,9,2022) AS psi_ready,
date(5,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
null AS remaining_payment_acquired,
null AS remaining_payment_acquired,
date(7,9,2022) AS shipped
union all
select
'KWW72_YLLPW' AS friendly_id,
date(25,8,2022) AS advance_payment_requested,
date(25,8,2022) AS advance_payment_in_progress,
date(25,8,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(31,8,2022) AS ready_for_psi,
date(31,8,2022) AS ready_for_psi,
date(31,8,2022) AS psi_ready,
date(1,9,2022) AS client_gave_feedback,
date(1,9,2022) AS problems_are_fixed,
date(6,9,2022) AS remaining_payment_in_progress,
date(6,9,2022) AS remaining_payment_acquired,
date(9,9,2022) AS shipped
union all
select
'KWW72_YNN2L' AS friendly_id,
date(25,8,2022) AS advance_payment_requested,
date(25,8,2022) AS advance_payment_in_progress,
date(25,8,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(1,9,2022) AS ready_for_psi,
date(1,9,2022) AS ready_for_psi,
date(1,9,2022) AS psi_ready,
date(1,9,2022) AS client_gave_feedback,
date(5,9,2022) AS problems_are_fixed,
date(6,9,2022) AS remaining_payment_in_progress,
date(6,9,2022) AS remaining_payment_acquired,
date(9,9,2022) AS shipped
union all
select
'XEJK3' AS friendly_id,
date(20,6,2022) AS advance_payment_requested,
date(21,6,2022) AS advance_payment_in_progress,
date(22,6,2022) AS advance_payment_acquired,
60 AS manufacturing_and_qc_in_progress,
date(16,8,2022) AS ready_for_psi,
date(16,8,2022) AS ready_for_psi,
date(19,8,2022) AS psi_ready,
date(22,8,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(22,8,2022) AS remaining_payment_in_progress,
date(23,8,2022) AS remaining_payment_acquired,
date(10,9,2022) AS shipped
union all
select
'X5RV3' AS friendly_id,
date(30,6,2022) AS advance_payment_requested,
date(1,7,2022) AS advance_payment_in_progress,
date(3,7,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(8,8,2022) AS ready_for_psi,
date(8,8,2022) AS ready_for_psi,
date(8,8,2022) AS psi_ready,
date(11,8,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(11,8,2022) AS remaining_payment_in_progress,
date(13,8,2022) AS remaining_payment_acquired,
date(10,9,2022) AS shipped
union all
select
'J39RY' AS friendly_id,
date(31,8,2022) AS advance_payment_requested,
date(31,8,2022) AS advance_payment_in_progress,
date(31,8,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(10,9,2022) AS ready_for_psi,
date(10,9,2022) AS ready_for_psi,
date(12,9,2022) AS psi_ready,
date(14,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(14,9,2022) AS remaining_payment_in_progress,
date(14,9,2022) AS remaining_payment_acquired,
date(15,9,2022) AS shipped
union all
select
'XQMDY' AS friendly_id,
date(7,9,2022) AS advance_payment_requested,
date(30,8,2022) AS advance_payment_in_progress,
date(1,9,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(6,9,2022) AS ready_for_psi,
date(6,9,2022) AS ready_for_psi,
date(7,9,2022) AS psi_ready,
date(7,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(12,9,2022) AS remaining_payment_in_progress,
date(14,9,2022) AS remaining_payment_acquired,
date(15,9,2022) AS shipped
union all
select
'KE7XR' AS friendly_id,
date(22,8,2022) AS advance_payment_requested,
date(22,8,2022) AS advance_payment_in_progress,
date(23,8,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(4,9,2022) AS ready_for_psi,
date(4,9,2022) AS ready_for_psi,
date(5,9,2022) AS psi_ready,
date(7,9,2022) AS client_gave_feedback,
date(13,9,2022) AS problems_are_fixed,
date(14,9,2022) AS remaining_payment_in_progress,
date(14,9,2022) AS remaining_payment_acquired,
date(16,9,2022) AS shipped
union all
select
'3D4GL' AS friendly_id,
date(29,8,2022) AS advance_payment_requested,
date(30,8,2022) AS advance_payment_in_progress,
date(31,8,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(6,9,2022) AS ready_for_psi,
date(6,9,2022) AS ready_for_psi,
date(6,9,2022) AS psi_ready,
date(6,9,2022) AS client_gave_feedback,
date(13,9,2022) AS problems_are_fixed,
date(15,9,2022) AS remaining_payment_in_progress,
date(16,9,2022) AS remaining_payment_acquired,
date(17,9,2022) AS shipped
union all
select
'J2QYV' AS friendly_id,
date(15,9,2022) AS advance_payment_requested,
date(15,9,2022) AS advance_payment_in_progress,
date(16,9,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(16,9,2022) AS ready_for_psi,
date(16,9,2022) AS ready_for_psi,
date(20,9,2022) AS psi_ready,
date(20,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(5,10,2022) AS remaining_payment_in_progress,
date(6,10,2022) AS remaining_payment_acquired,
date(21,9,2022) AS shipped
union all
select
'X49KL' AS friendly_id,
date(26,8,2022) AS advance_payment_requested,
date(26,8,2022) AS advance_payment_in_progress,
date(26,8,2022) AS advance_payment_acquired,
30 AS manufacturing_and_qc_in_progress,
date(16,9,2022) AS ready_for_psi,
date(16,9,2022) AS ready_for_psi,
date(16,9,2022) AS psi_ready,
date(19,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(19,9,2022) AS remaining_payment_in_progress,
date(19,9,2022) AS remaining_payment_acquired,
date(21,9,2022) AS shipped
union all
select
'KMQ98' AS friendly_id,
date(26,8,2022) AS advance_payment_requested,
date(30,8,2022) AS advance_payment_in_progress,
date(1,9,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(19,9,2022) AS ready_for_psi,
date(19,9,2022) AS ready_for_psi,
date(20,9,2022) AS psi_ready,
date(20,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(21,9,2022) AS remaining_payment_in_progress,
date(22,9,2022) AS remaining_payment_acquired,
date(23,9,2022) AS shipped
union all
select
'KGLEL' AS friendly_id,
date(22,8,2022) AS advance_payment_requested,
date(22,8,2022) AS advance_payment_in_progress,
date(24,8,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(19,9,2022) AS ready_for_psi,
date(19,9,2022) AS ready_for_psi,
date(19,9,2022) AS psi_ready,
date(20,9,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(22,9,2022) AS remaining_payment_in_progress,
date(26,9,2022) AS remaining_payment_acquired,
date(27,9,2022) AS shipped
union all
select
'3KR5X' AS friendly_id,
date(29,8,2022) AS advance_payment_requested,
date(6,9,2022) AS advance_payment_in_progress,
date(7,9,2022) AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
date(27,8,2022) AS ready_for_psi,
date(27,8,2022) AS ready_for_psi,
date(28,8,2022) AS psi_ready,
date(28,8,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(30,9,2022) AS remaining_payment_in_progress,
date(30,9,2022) AS remaining_payment_acquired,
date(8,10,2022) AS shipped
union all
select
'J4QE6' AS friendly_id,
date(20,9,2022) AS advance_payment_requested,
date(20,9,2022) AS advance_payment_in_progress,
date(21,9,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(10,10,2022) AS ready_for_psi,
date(10,10,2022) AS ready_for_psi,
date(10,10,2022) AS psi_ready,
date(10,10,2022) AS client_gave_feedback,
date(18,10,2022) AS problems_are_fixed,
date(18,10,2022) AS remaining_payment_in_progress,
date(19,10,2022) AS remaining_payment_acquired,
date(21,10,2022) AS shipped
union all
select
'J2Q29' AS friendly_id,
date(20,9,2022) AS advance_payment_requested,
date(20,9,2022) AS advance_payment_in_progress,
date(22,9,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(17,10,2022) AS ready_for_psi,
date(17,10,2022) AS ready_for_psi,
date(17,10,2022) AS psi_ready,
date(18,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(19,10,2022) AS remaining_payment_in_progress,
date(21,10,2022) AS remaining_payment_acquired,
date(22,10,2022) AS shipped
union all
select
'K6DZX' AS friendly_id,
date(22,9,2022) AS advance_payment_requested,
date(26,9,2022) AS advance_payment_in_progress,
date(28,9,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(10,10,2022) AS ready_for_psi,
date(10,10,2022) AS ready_for_psi,
date(11,10,2022) AS psi_ready,
date(11,10,2022) AS client_gave_feedback,
date(12,10,2022) AS problems_are_fixed,
date(13,10,2022) AS remaining_payment_in_progress,
date(14,10,2022) AS remaining_payment_acquired,
date(23,10,2022) AS shipped
union all
select
'JV349' AS friendly_id,
date(29,9,2022) AS advance_payment_requested,
date(30,9,2022) AS advance_payment_in_progress,
date(1,10,2022) AS advance_payment_acquired,
22 AS manufacturing_and_qc_in_progress,
date(10,19,2022) AS ready_for_psi,
date(10,19,2022) AS ready_for_psi,
date(20,10,2022) AS psi_ready,
date(20,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(20,10,2022) AS remaining_payment_in_progress,
date(24,10,2022) AS remaining_payment_acquired,
date(25,10,2022) AS shipped
union all
select
'KDX4M_Y4VE9' AS friendly_id,
date(5,10,2022) AS advance_payment_requested,
date(6,10,2022) AS advance_payment_in_progress,
date(6,10,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(22,10,2022) AS ready_for_psi,
date(22,10,2022) AS ready_for_psi,
date(24,10,2022) AS psi_ready,
date(24,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(24,10,2022) AS remaining_payment_in_progress,
date(25,10,2022) AS remaining_payment_acquired,
date(26,10,2022) AS shipped
union all
select
'KDX4M_G57QX' AS friendly_id,
date(5,10,2022) AS advance_payment_requested,
date(6,10,2022) AS advance_payment_in_progress,
date(6,10,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(12,10,2022) AS ready_for_psi,
date(12,10,2022) AS ready_for_psi,
date(14,10,2022) AS psi_ready,
date(17,10,2022) AS client_gave_feedback,
date(19,10,2022) AS problems_are_fixed,
date(19,10,2022) AS remaining_payment_in_progress,
date(19,10,2022) AS remaining_payment_acquired,
date(26,10,2022) AS shipped
union all
select
'JZYL9' AS friendly_id,
date(29,8,2022) AS advance_payment_requested,
date(6,9,2022) AS advance_payment_in_progress,
date(6,9,2022) AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
date(27,9,2022) AS ready_for_psi,
date(27,9,2022) AS ready_for_psi,
date(19,10,2022) AS psi_ready,
date(20,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(20,10,2022) AS remaining_payment_in_progress,
date(20,10,2022) AS remaining_payment_acquired,
date(26,10,2022) AS shipped
union all
select
'K5VPE' AS friendly_id,
date(3,10,2022) AS advance_payment_requested,
date(4,10,2022) AS advance_payment_in_progress,
date(6,10,2022) AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
date(20,10,2022) AS ready_for_psi,
date(20,10,2022) AS ready_for_psi,
date(21,10,2022) AS psi_ready,
date(24,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(24,10,2022) AS remaining_payment_in_progress,
date(25,10,2022) AS remaining_payment_acquired,
date(27,10,2022) AS shipped
union all
select
'K5V88' AS friendly_id,
date(07,10,2022) AS advance_payment_requested,
date(13,10,2022) AS advance_payment_in_progress,
date(14,10,2022) AS advance_payment_acquired,
7 AS manufacturing_and_qc_in_progress,
date(24,10,2022) AS ready_for_psi,
date(24,10,2022) AS ready_for_psi,
date(24,10,2022) AS psi_ready,
date(25,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(26,10,2022) AS remaining_payment_in_progress,
date(27,10,2022) AS remaining_payment_acquired,
date(28,10,2022) AS shipped
union all
select
'KGRDV' AS friendly_id,
date(5,10,2022) AS advance_payment_requested,
date(6,10,2022) AS advance_payment_in_progress,
date(11,10,2022) AS advance_payment_acquired,
11 AS manufacturing_and_qc_in_progress,
date(24,10,2022) AS ready_for_psi,
date(24,10,2022) AS ready_for_psi,
date(24,10,2022) AS psi_ready,
date(27,10,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(27,10,2022) AS remaining_payment_in_progress,
date(31,10,2022) AS remaining_payment_acquired,
date(31,10,2022) AS shipped
union all
select
'JQZ5M' AS friendly_id,
date(14,10,2022) AS advance_payment_requested,
date(17,10,2022) AS advance_payment_in_progress,
date(19,10,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(29,10,2022) AS ready_for_psi,
date(29,10,2022) AS ready_for_psi,
date(3,11,2022) AS psi_ready,
date(3,11,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(7,11,2022) AS remaining_payment_in_progress,
date(9,11,2022) AS remaining_payment_acquired,
date(10,11,2022) AS shipped
union all
select
'JV3XE' AS friendly_id,
date(19,10,2022) AS advance_payment_requested,
date(20,10,2022) AS advance_payment_in_progress,
date(21,10,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(3,11,2022) AS ready_for_psi,
date(3,11,2022) AS ready_for_psi,
date(3,11,2022) AS psi_ready,
date(8,11,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(8,11,2022) AS remaining_payment_in_progress,
date(9,11,2022) AS remaining_payment_acquired,
date(10,11,2022) AS shipped
union all
select
'3DDLN' AS friendly_id,
date(3,10,2022) AS advance_payment_requested,
date(30,9,2022) AS advance_payment_in_progress,
date(10,10,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(20,10,2022) AS ready_for_psi,
date(20,10,2022) AS ready_for_psi,
date(20,10,2022) AS psi_ready,
date(21,10,2022) AS client_gave_feedback,
date(7,11,2022) AS problems_are_fixed,
date(20,10,2022) AS remaining_payment_in_progress,
date(21,10,2022) AS remaining_payment_acquired,
date(12,11,2022) AS shipped
union all
select
'J9979' AS friendly_id,
date(31,10,2022) AS advance_payment_requested,
date(28,10,2022) AS advance_payment_in_progress,
date(31,10,2022) AS advance_payment_acquired,
10 AS manufacturing_and_qc_in_progress,
date(10,11,2022) AS ready_for_psi,
date(10,11,2022) AS ready_for_psi,
date(10,11,2022) AS psi_ready,
date(14,11,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(14,11,2022) AS remaining_payment_in_progress,
date(16,11,2022) AS remaining_payment_acquired,
date(18,11,2022) AS shipped
union all
select
'KLGNE' AS friendly_id,
date(7,10,2022) AS advance_payment_requested,
date(10,10,2022) AS advance_payment_in_progress,
date(11,10,2022) AS advance_payment_acquired,
30 AS manufacturing_and_qc_in_progress,
date(7,11,2022) AS ready_for_psi,
date(7,11,2022) AS ready_for_psi,
date(7,11,2022) AS psi_ready,
date(8,11,2022) AS client_gave_feedback,
date(9,11,2022) AS problems_are_fixed,
date(10,11,2022) AS remaining_payment_in_progress,
date(12,11,2022) AS remaining_payment_acquired,
date(22,11,2022) AS shipped
union all
select
'KLZWL' AS friendly_id,
date(31,10,2022) AS advance_payment_requested,
date(28,10,2022) AS advance_payment_in_progress,
date(30,10,2022) AS advance_payment_acquired,
15 AS manufacturing_and_qc_in_progress,
date(15,11,2022) AS ready_for_psi,
date(15,11,2022) AS ready_for_psi,
date(16,11,2022) AS psi_ready,
date(17,11,2022) AS client_gave_feedback,
date(21,11,2022) AS problems_are_fixed,
date(23,11,2022) AS remaining_payment_in_progress,
date(24,11,2022) AS remaining_payment_acquired,
date(25,11,2022) AS shipped
union all
select
'JYDE2' AS friendly_id,
date(5,10,2022) AS advance_payment_requested,
date(6,10,2022) AS advance_payment_in_progress,
date(11,10,2022) AS advance_payment_acquired,
16 AS manufacturing_and_qc_in_progress,
date(27,10,2022) AS ready_for_psi,
date(27,10,2022) AS ready_for_psi,
date(21,11,2022) AS psi_ready,
date(23,11,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(23,11,2022) AS remaining_payment_in_progress,
date(24,11,2022) AS remaining_payment_acquired,
date(26,11,2022) AS shipped
union all
select
'JNEVD' AS friendly_id,
date(31,10,2022) AS advance_payment_requested,
date(28,10,2022) AS advance_payment_in_progress,
date(29,10,2022) AS advance_payment_acquired,
20 AS manufacturing_and_qc_in_progress,
date(21,11,2022) AS ready_for_psi,
date(21,11,2022) AS ready_for_psi,
date(21,11,2022) AS psi_ready,
date(22,11,2022) AS client_gave_feedback,
null AS remaining_payment_in_progress,
date(23,11,2022) AS remaining_payment_in_progress,
date(24,11,2022) AS remaining_payment_acquired,
date(30,11,2022) AS shipped
