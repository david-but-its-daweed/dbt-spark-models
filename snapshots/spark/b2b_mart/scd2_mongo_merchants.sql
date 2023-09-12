{% snapshot scd2_mongo_merchants %}

{{
    config(
    meta = {
      'model_owner' : '@amitiushkina'
    },
      target_schema='b2b_mart',
      unique_key='merchant_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True,
    )
}}



WITH jp_merchants AS (
    SELECT DISTINCT _id AS merchant_id,
    jpMerchant AS jp_merchant
    FROM {{ source('mongo', 'b2b_core_merchant_appendixes_daily_snapshot') }}
)

SELECT m._id AS                              merchant_id,
       accountsEnabled AS account_enabled,
       activatedByMerchant as activated_by_merchant,
       millis_to_ts_msk(activationTimeMs) as activated_time_ts,
       millis_to_ts_msk(blockActionTime) AS block_time,
       companyName AS company_name,
       millis_to_ts_msk(createdTimeMs) as created_ts_msk,
       millis_to_ts_msk(delayedBlockTime) as delayed_block_time,
       disablingNote AS disabling_note,
       disablingReason AS disabling_reason,
       enabled,
       isJoom AS is_joom,
       jp_merchant,
       isNew AS is_new,
       lead,
       leadCommission AS lead_comission,
       name,
       millis_to_ts_msk(unblockTime) AS unblock_time,
       millis_to_ts_msk(updatedTimeMs) AS update_ts_msk

from {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }} m
LEFT JOIN jp_merchants jp ON m._id = jp.merchant_id

{% endsnapshot %}
