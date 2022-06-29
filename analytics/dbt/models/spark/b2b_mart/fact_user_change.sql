{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'true'
    }
) }}

SELECT event_id,
    partition_date AS partition_date_msk,
    event_ts_msk AS event_ts_msk,
    payload.reason AS change_type,
    payload.roleSet.roles.`owner`.actualisationTime.time AS owner_ts_utc,
    payload.roleSet.roles.`owner`.moderatorId AS owner_moderator_id,
    payload.roleSet.roles.`bizDev`.actualisationTime.time AS biz_dev_ts_utc,
    payload.roleSet.roles.`bizDev`.moderatorId as biz_dev_moderator_id,
    payload.userId AS user_id,
    payload.validationRejectReason AS reject_reason,
    payload.validationStatus AS validation_status
from  {{ source('b2b_mart', 'operational_events') }}
where `type` ='customerUpdated'
{% if is_incremental() %}
  and partition_date >= date'{{ var("start_date_ymd") }}'
  and partition_date < date'{{ var("end_date_ymd") }}'
{% else %}
  and partition_date   >= date'2022-05-19'
{% endif %}

