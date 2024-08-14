{{
  config(
    materialized='table',
    meta = {
        'model_owner' : '@catman-analytics.duty'
    },
  )
}}

SELECT
  *
FROM junk_yatsushko.fbj_table_for_backfill_kirill