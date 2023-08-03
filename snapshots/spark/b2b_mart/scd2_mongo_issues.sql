{% snapshot scd2_mongo_issues %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='issue_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}




select _id as issue_id,
assigneeId as assignee_id,
entityId as entity_id,
friendlyId as issue_friendly_id,
note,
parentId as issue_parent_id,
priority,
reporterId as reporter_id,
statusHistory as status_history,
assigneeHistory as assignee_history,
type,
millis_to_ts_msk(ctms) AS created_ts_msk,
millis_to_ts_msk(utms+1) AS update_ts_msk

from {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}

{% endsnapshot %}
