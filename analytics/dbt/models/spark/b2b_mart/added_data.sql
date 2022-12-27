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
'15.07.2022' AS advance_payment_requested,
'19.07.2022' AS advance_payment_in_progress,
'19.07.2022' AS advance_payment_acquired,
5 AS manufacturing_days,
'20.07.2022' AS ready_for_psi,
'23.07.2022' AS psi_ready,
'23.07.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'01.08.2022' AS remaining_payment_in_progress,
'01.08.2022' AS remaining_payment_acquired,
'03.08.2022' AS shipped
union all
select
'XN2QX_XZZVX' AS friendly_id,
'15.07.2022' AS advance_payment_requested,
'19.07.2022' AS advance_payment_in_progress,
'21.07.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'29.07.2022' AS ready_for_psi,
'29.07.2022' AS psi_ready,
'01.08.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'01.08.2022' AS remaining_payment_in_progress,
'02.08.2022' AS remaining_payment_acquired,
'03.08.2022' AS shipped
union all
select
'3W27G' AS friendly_id,
'07.07.2022' AS advance_payment_requested,
'11.07.2022' AS advance_payment_in_progress,
'12.07.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'31.07.2022' AS ready_for_psi,
'03.08.2022' AS psi_ready,
'08.08.2022' AS client_gave_feedback,
'08.08.2022' AS problems_are_fixed,
'10.08.2022' AS remaining_payment_in_progress,
'11.08.2022' AS remaining_payment_acquired,
'15.08.2022' AS shipped
union all
select
'XJKWQ' AS friendly_id,
'28.07.2022' AS advance_payment_requested,
'28.07.2022' AS advance_payment_in_progress,
'29.07.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'08.08.2022' AS ready_for_psi,
'09.08.2022' AS psi_ready,
'11.08.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'11.08.2022' AS remaining_payment_in_progress,
'15.08.2022' AS remaining_payment_acquired,
'16.08.2022' AS shipped
union all
select
'XGNWM' AS friendly_id,
'12.08.2022' AS advance_payment_requested,
'08.08.2022' AS advance_payment_in_progress,
'10.08.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'14.08.2022' AS ready_for_psi,
'15.08.2022' AS psi_ready,
'16.08.2022' AS client_gave_feedback,
'16.08.2022' AS problems_are_fixed,
'18.08.2022' AS remaining_payment_in_progress,
'19.08.2022' AS remaining_payment_acquired,
'21.08.2022' AS shipped
union all
select
'XNW7Q' AS friendly_id,
'25.08.2022' AS advance_payment_requested,
'05.08.2022' AS advance_payment_in_progress,
'10.08.2022' AS advance_payment_acquired,
17 AS manufacturing_days,
'29.08.2022' AS ready_for_psi,
'29.08.2022' AS psi_ready,
'29.08.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'30.08.2022' AS remaining_payment_in_progress,
'31.08.2022' AS remaining_payment_acquired,
'01.09.2022' AS shipped
union all
select
'KRE4P' AS friendly_id,
'26.08.2022' AS advance_payment_requested,
'22.08.2022' AS advance_payment_in_progress,
'23.08.2022' AS advance_payment_acquired,
30 AS manufacturing_days,
'01.09.2022' AS ready_for_psi,
'02.09.2022' AS psi_ready,
'05.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'' AS remaining_payment_in_progress,
'' AS remaining_payment_acquired,
'07.09.2022' AS shipped
union all
select
'KWW72_YLLPW' AS friendly_id,
'25.08.2022' AS advance_payment_requested,
'25.08.2022' AS advance_payment_in_progress,
'25.08.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'31.08.2022' AS ready_for_psi,
'31.08.2022' AS psi_ready,
'01.09.2022' AS client_gave_feedback,
'01.09.2022' AS problems_are_fixed,
'06.09.2022' AS remaining_payment_in_progress,
'06.09.2022' AS remaining_payment_acquired,
'09.09.2022' AS shipped
union all
select
'KWW72_YNN2L' AS friendly_id,
'25.08.2022' AS advance_payment_requested,
'25.08.2022' AS advance_payment_in_progress,
'25.08.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'01.09.2022' AS ready_for_psi,
'01.09.2022' AS psi_ready,
'01.09.2022' AS client_gave_feedback,
'05.09.2022' AS problems_are_fixed,
'06.09.2022' AS remaining_payment_in_progress,
'06.09.2022' AS remaining_payment_acquired,
'09.09.2022' AS shipped
union all
select
'XEJK3' AS friendly_id,
'20.06.2022' AS advance_payment_requested,
'21.06.2022' AS advance_payment_in_progress,
'22.06.2022' AS advance_payment_acquired,
60 AS manufacturing_days,
'16.08.2022' AS ready_for_psi,
'19.08.2022' AS psi_ready,
'22.08.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'22.08.2022' AS remaining_payment_in_progress,
'23.08.2022' AS remaining_payment_acquired,
'10.09.2022' AS shipped
union all
select
'X5RV3' AS friendly_id,
'30.06.2022' AS advance_payment_requested,
'01.07.2022' AS advance_payment_in_progress,
'03.07.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'08.08.2022' AS ready_for_psi,
'08.08.2022' AS psi_ready,
'11.08.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'11.08.2022' AS remaining_payment_in_progress,
'13.08.2022' AS remaining_payment_acquired,
'10.09.2022' AS shipped
union all
select
'J39RY' AS friendly_id,
'31.08.2022' AS advance_payment_requested,
'31.08.2022' AS advance_payment_in_progress,
'31.08.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'10.09.2022' AS ready_for_psi,
'12.09.2022' AS psi_ready,
'14.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'14.09.2022' AS remaining_payment_in_progress,
'14.09.2022' AS remaining_payment_acquired,
'15.09.2022' AS shipped
union all
select
'XQMDY' AS friendly_id,
'07.09.2022' AS advance_payment_requested,
'30.08.2022' AS advance_payment_in_progress,
'01.09.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'06.09.2022' AS ready_for_psi,
'07.09.2022' AS psi_ready,
'07.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'12.09.2022' AS remaining_payment_in_progress,
'14.09.2022' AS remaining_payment_acquired,
'15.09.2022' AS shipped
union all
select
'KE7XR' AS friendly_id,
'22.08.2022' AS advance_payment_requested,
'22.08.2022' AS advance_payment_in_progress,
'23.08.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'04.09.2022' AS ready_for_psi,
'05.09.2022' AS psi_ready,
'07.09.2022' AS client_gave_feedback,
'13.09.2022' AS problems_are_fixed,
'14.09.2022' AS remaining_payment_in_progress,
'14.09.2022' AS remaining_payment_acquired,
'16.09.2022' AS shipped
union all
select
'3D4GL' AS friendly_id,
'29.08.2022' AS advance_payment_requested,
'30.08.2022' AS advance_payment_in_progress,
'31.08.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'06.09.2022' AS ready_for_psi,
'06.09.2022' AS psi_ready,
'06.09.2022' AS client_gave_feedback,
'13.09.2022' AS problems_are_fixed,
'15.09.2022' AS remaining_payment_in_progress,
'16.09.2022' AS remaining_payment_acquired,
'17.09.2022' AS shipped
union all
select
'J2QYV' AS friendly_id,
'15.09.2022' AS advance_payment_requested,
'15.09.2022' AS advance_payment_in_progress,
'16.09.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'16.09.2022' AS ready_for_psi,
'20.09.2022' AS psi_ready,
'20.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'05.10.2022' AS remaining_payment_in_progress,
'06.10.2022' AS remaining_payment_acquired,
'21.09.2022' AS shipped
union all
select
'X49KL' AS friendly_id,
'26.08.2022' AS advance_payment_requested,
'26.08.2022' AS advance_payment_in_progress,
'26.08.2022' AS advance_payment_acquired,
30 AS manufacturing_days,
'16.09.2022' AS ready_for_psi,
'16.09.2022' AS psi_ready,
'19.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'19.09.2022' AS remaining_payment_in_progress,
'19.09.2022' AS remaining_payment_acquired,
'21.09.2022' AS shipped
union all
select
'KMQ98' AS friendly_id,
'26.08.2022' AS advance_payment_requested,
'30.08.2022' AS advance_payment_in_progress,
'01.09.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'19.09.2022' AS ready_for_psi,
'20.09.2022' AS psi_ready,
'20.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'21.09.2022' AS remaining_payment_in_progress,
'22.09.2022' AS remaining_payment_acquired,
'23.09.2022' AS shipped
union all
select
'KGLEL' AS friendly_id,
'22.08.2022' AS advance_payment_requested,
'22.08.2022' AS advance_payment_in_progress,
'24.08.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'19.09.2022' AS ready_for_psi,
'19.09.2022' AS psi_ready,
'20.09.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'22.09.2022' AS remaining_payment_in_progress,
'26.09.2022' AS remaining_payment_acquired,
'27.09.2022' AS shipped
union all
select
'3KR5X' AS friendly_id,
'29.08.2022' AS advance_payment_requested,
'06.09.2022' AS advance_payment_in_progress,
'07.09.2022' AS advance_payment_acquired,
20 AS manufacturing_days,
'27.08.2022' AS ready_for_psi,
'28.08.2022' AS psi_ready,
'28.08.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'30.09.2022' AS remaining_payment_in_progress,
'30.09.2022' AS remaining_payment_acquired,
'08.10.2022' AS shipped
union all
select
'J4QE6' AS friendly_id,
'20.09.2022' AS advance_payment_requested,
'20.09.2022' AS advance_payment_in_progress,
'21.09.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'10.10.2022' AS ready_for_psi,
'10.10.2022' AS psi_ready,
'10.10.2022' AS client_gave_feedback,
'18.10.2022' AS problems_are_fixed,
'18.10.2022' AS remaining_payment_in_progress,
'19.10.2022' AS remaining_payment_acquired,
'21.10.2022' AS shipped
union all
select
'J2Q29' AS friendly_id,
'20.09.2022' AS advance_payment_requested,
'20.09.2022' AS advance_payment_in_progress,
'22.09.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'17.10.2022' AS ready_for_psi,
'17.10.2022' AS psi_ready,
'18.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'19.10.2022' AS remaining_payment_in_progress,
'21.10.2022' AS remaining_payment_acquired,
'22.10.2022' AS shipped
union all
select
'K6DZX' AS friendly_id,
'22.09.2022' AS advance_payment_requested,
'26.09.2022' AS advance_payment_in_progress,
'28.09.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'10.10.2022' AS ready_for_psi,
'11.10.2022' AS psi_ready,
'11.10.2022' AS client_gave_feedback,
'12.10.2022' AS problems_are_fixed,
'13.10.2022' AS remaining_payment_in_progress,
'14.10.2022' AS remaining_payment_acquired,
'23.10.2022' AS shipped
union all
select
'JV349' AS friendly_id,
'29.09.2022' AS advance_payment_requested,
'30.09.2022' AS advance_payment_in_progress,
'01.10.2022' AS advance_payment_acquired,
22 AS manufacturing_days,
'19/10/2022' AS ready_for_psi,
'20.10.2022' AS psi_ready,
'20.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'20.10.2022' AS remaining_payment_in_progress,
'24.10.2022' AS remaining_payment_acquired,
'25.10.2022' AS shipped
union all
select
'KDX4M_Y4VE9' AS friendly_id,
'05.10.2022' AS advance_payment_requested,
'06.10.2022' AS advance_payment_in_progress,
'06.10.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'22.10.2022' AS ready_for_psi,
'24.10.2022' AS psi_ready,
'24.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'24.10.2022' AS remaining_payment_in_progress,
'25.10.2022' AS remaining_payment_acquired,
'26.10.2022' AS shipped
union all
select
'KDX4M_G57QX' AS friendly_id,
'05.10.2022' AS advance_payment_requested,
'06.10.2022' AS advance_payment_in_progress,
'06.10.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'12.10.2022' AS ready_for_psi,
'14.10.2022' AS psi_ready,
'17.10.2022' AS client_gave_feedback,
'19.10.2022' AS problems_are_fixed,
'19.10.2022' AS remaining_payment_in_progress,
'19.10.2022' AS remaining_payment_acquired,
'26.10.2022' AS shipped
union all
select
'JZYL9' AS friendly_id,
'29.08.2022' AS advance_payment_requested,
'06.09.2022' AS advance_payment_in_progress,
'06.09.2022' AS advance_payment_acquired,
20 AS manufacturing_days,
'27.09.2022' AS ready_for_psi,
'19.10.2022' AS psi_ready,
'20.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'20.10.2022' AS remaining_payment_in_progress,
'20.10.2022' AS remaining_payment_acquired,
'26.10.2022' AS shipped
union all
select
'K5VPE' AS friendly_id,
'03.10.2022' AS advance_payment_requested,
'04.10.2022' AS advance_payment_in_progress,
'06.10.2022' AS advance_payment_acquired,
20 AS manufacturing_days,
'20.10.2022' AS ready_for_psi,
'21.10.2022' AS psi_ready,
'24.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'24.10.2022' AS remaining_payment_in_progress,
'25.10.2022' AS remaining_payment_acquired,
'27.10.2022' AS shipped
union all
select
'K5V88' AS friendly_id,
'07.10.2022' AS advance_payment_requested,
'13.10.2022' AS advance_payment_in_progress,
'14.10.2022' AS advance_payment_acquired,
7 AS manufacturing_days,
'24.10.2022' AS ready_for_psi,
'24.10.2022' AS psi_ready,
'25.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'26.10.2022' AS remaining_payment_in_progress,
'27.10.2022' AS remaining_payment_acquired,
'28.10.2022' AS shipped
union all
select
'KGRDV' AS friendly_id,
'05.10.2022' AS advance_payment_requested,
'06.10.2022' AS advance_payment_in_progress,
'11.10.2022' AS advance_payment_acquired,
11 AS manufacturing_days,
'24.10.2022' AS ready_for_psi,
'24.10.2022' AS psi_ready,
'27.10.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'27.10.2022' AS remaining_payment_in_progress,
'31.10.2022' AS remaining_payment_acquired,
'31.10.2022' AS shipped
union all
select
'JQZ5M' AS friendly_id,
'14.10.2022' AS advance_payment_requested,
'17.10.2022' AS advance_payment_in_progress,
'19.10.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'29.10.2022' AS ready_for_psi,
'03.11.2022' AS psi_ready,
'03.11.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'07.11.2022' AS remaining_payment_in_progress,
'09.11.2022' AS remaining_payment_acquired,
'10.11.2022' AS shipped
union all
select
'JV3XE' AS friendly_id,
'19.10.2022' AS advance_payment_requested,
'20.10.2022' AS advance_payment_in_progress,
'21.10.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'03.11.2022' AS ready_for_psi,
'03.11.2022' AS psi_ready,
'08.11.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'08.11.2022' AS remaining_payment_in_progress,
'09.11.2022' AS remaining_payment_acquired,
'10.11.2022' AS shipped
union all
select
'3DDLN' AS friendly_id,
'03.10.2022' AS advance_payment_requested,
'30.09.2022' AS advance_payment_in_progress,
'10.10.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'20.10.2022' AS ready_for_psi,
'20.10.2022' AS psi_ready,
'21.10.2022' AS client_gave_feedback,
'07.11.2022' AS problems_are_fixed,
'20.10.2022' AS remaining_payment_in_progress,
'21.10.2022' AS remaining_payment_acquired,
'12.11.2022' AS shipped
union all
select
'J9979' AS friendly_id,
'31.10.2022' AS advance_payment_requested,
'28.10.2022' AS advance_payment_in_progress,
'31.10.2022' AS advance_payment_acquired,
10 AS manufacturing_days,
'10.11.2022' AS ready_for_psi,
'10.11.2022' AS psi_ready,
'14.11.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'14.11.2022' AS remaining_payment_in_progress,
'16.11.2022' AS remaining_payment_acquired,
'18.11.2022' AS shipped
union all
select
'KLGNE' AS friendly_id,
'07.10.2022' AS advance_payment_requested,
'10.10.2022' AS advance_payment_in_progress,
'11.10.2022' AS advance_payment_acquired,
30 AS manufacturing_days,
'07.11.2022' AS ready_for_psi,
'07.11.2022' AS psi_ready,
'08.11.2022' AS client_gave_feedback,
'09.11.2022' AS problems_are_fixed,
'10.11.2022' AS remaining_payment_in_progress,
'12.11.2022' AS remaining_payment_acquired,
'22.11.2022' AS shipped
union all
select
'KLZWL' AS friendly_id,
'31.10.2022' AS advance_payment_requested,
'28.10.2022' AS advance_payment_in_progress,
'30.10.2022' AS advance_payment_acquired,
15 AS manufacturing_days,
'15.11.2022' AS ready_for_psi,
'16.11.2022' AS psi_ready,
'17.11.2022' AS client_gave_feedback,
'21.11.2022' AS problems_are_fixed,
'23.11.2022' AS remaining_payment_in_progress,
'24.11.2022' AS remaining_payment_acquired,
'25.11.2022' AS shipped
union all
select
'JYDE2' AS friendly_id,
'05.10.2022' AS advance_payment_requested,
'06.10.2022' AS advance_payment_in_progress,
'11.10.2022' AS advance_payment_acquired,
16 AS manufacturing_days,
'27.10.2022' AS ready_for_psi,
'21.11.2022' AS psi_ready,
'23.11.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'23.11.2022' AS remaining_payment_in_progress,
'24.11.2022' AS remaining_payment_acquired,
'26.11.2022' AS shipped
union all
select
'JNEVD' AS friendly_id,
'31.10.2022' AS advance_payment_requested,
'28.10.2022' AS advance_payment_in_progress,
'29.10.2022' AS advance_payment_acquired,
20 AS manufacturing_days,
'21.11.2022' AS ready_for_psi,
'21.11.2022' AS psi_ready,
'22.11.2022' AS client_gave_feedback,
'' AS problems_are_fixed,
'23.11.2022' AS remaining_payment_in_progress,
'24.11.2022' AS remaining_payment_acquired,
'30.11.2022' AS shipped
