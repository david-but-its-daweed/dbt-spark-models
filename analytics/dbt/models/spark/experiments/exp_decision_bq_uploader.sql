{{ config(
    schema='experiments',
    materialized='view',
    meta = {
      'team': 'platform',
      'model_owner': '@logrel',
      'bigquery_load': 'true',
      'bigquery_table_name': 'experiments.decisions',
      'bigquery_partitioning_date_column': 'finish_date',
      'bigquery_overwrite': 'true'
    }
) }}

WITH exp_decisions AS (
SELECT
    CONCAT_WS('.', publicId, version) AS experiment_id,
    acceptanceType AS experiment_type,
    author AS author_id,
    state AS experiment_status,
    TO_DATE(TIMESTAMP_MILLIS(startTimeMs)) AS start_date,
    TO_DATE(TIMESTAMP_MILLIS(COALESCE(cancelledTimeMs, endTimeMs))) AS finish_date,
    decisionStatus.status AS decision_status,
    decisionStatus.description AS decision_description
FROM {{ source('experiments', 'experiments_setup') }}
WHERE
    NOT ARRAY_CONTAINS(commands, 'experimentPlatform')
    AND startTimeMs >= 1672531200000                   
    AND state IN ('finished', 'stopped')
),

rollout_group AS (
SELECT
    experiment_id,
    group_id AS rollout_group,
    rollout_date
FROM {{ source('experiments', 'rollout_group') }}
WHERE start_date >= '2023-01-01'
)

SELECT
    d.experiment_id,
    experiment_type,
    a.username AS author,
    experiment_status,
    start_date,
    finish_date,
    decision_status,
    decision_description,
    r.rollout_group,
    r.rollout_date
FROM exp_decisions AS d
LEFT JOIN experiments.authors_manual AS a USING(author_id) -- authors_manual is not updated regularly, thus no source
LEFT JOIN rollout_group AS r USING(experiment_id)