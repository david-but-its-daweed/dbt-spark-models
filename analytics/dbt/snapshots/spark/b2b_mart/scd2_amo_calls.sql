{% snapshot scd2_amo_calls %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='lead_id',

      strategy='timestamp',
      updated_at='created_ts_msk',
      file_format='delta'
    )
}}

with statuses as (
  SELECT "переговоры" AS sub_status,
  "negotiation" as status,
  3020 AS priority
  UNION ALL
  SELECT "Закрыто и не реализовано" AS sub_status,
  "cancelled" AS status,
  7010 AS priority
  UNION ALL
  SELECT "взят в работу" AS sub_status,
  "price estimation" AS status,
  1020 AS priority
  UNION ALL
  SELECT "отправлен запрос брокеру" AS sub_status,
  "price estimation" AS status,
  1040 AS priority
  UNION ALL
  SELECT "финальный расчет" AS sub_status,
  "final pricing" AS status,
  4010 AS priority
  UNION ALL
  SELECT "успешно реализовано (оплата)" AS sub_status,
  "signing and payment" as status,
  5030 AS priority
  UNION ALL
  SELECT "заявка на расчет" AS sub_status,
  "price estimation" AS status,
  1010 AS priority
  UNION ALL
  SELECT "поступила заявка" AS sub_status,
  "price estimation" AS status,
  1010 AS priority
  UNION ALL
  SELECT "запросы получены" AS sub_status,
  "price estimation" AS status,
  1050 AS priority
  UNION ALL
  SELECT "решение отложено" AS sub_status,
  "negotiation" as status,
  3030 AS priority
  UNION ALL
  SELECT "взяли в работу" AS sub_status,
  "price estimation" AS status,
  1020 AS priority
  UNION ALL
  SELECT "получена обратная связь" AS sub_status,
  "negotiation" as status,
  3010 AS priority
  UNION ALL
  SELECT "отправлен запрос биздеву" AS sub_status,
  "price estimation" AS status,
  1030 AS priority
  UNION ALL
  SELECT "кп отправлено" AS sub_status,
  "commercial offer" AS status,
  2020 AS priority
  UNION ALL
  SELECT "кп готово" AS sub_status,
  "commercial offer" AS status,
  2010 AS priority
  UNION ALL
  SELECT "договор согласован" AS sub_status,
  "contract" AS status,
  5010 AS priority
  UNION ALL
  SELECT "договор подписан" AS sub_status,
  "contract" AS status,
  5020 AS priority
  UNION ALL 
  SELECT "успешно реализовано (оплата)" AS sub_status,
  "payed" AS status,
  6010 AS priority
)


SELECT 
  callId AS call_id,
  callStatus AS call_status,
  millis_to_ts_msk(callTime) AS call_ts_msk,
  callerName AS owner_name,
  contactId AS amo_contact_id,
  millis_to_ts_msk(ctms) AS created_ts_msk,
  duration AS call_duration,
  leadId AS lead_id,
  s.status,
  s.sub_status,
  s.priority AS status_priority,
  skorozvonId AS skorozvon_id
FROM {{ source('mongo', 'b2b_core_amo_crm_calls_daily_snapshot') }} AS c
LEFT JOIN statuses AS s ON s.sub_status = c.leadStatus
{% endsnapshot %}
