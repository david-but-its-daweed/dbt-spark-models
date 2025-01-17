{{ config(
    schema='holistics',
    materialized='view',
    meta = {
      'model_owner' : '@analytics.duty',
      'bigquery_load': 'true',
      'bigquery_check_counts': 'false'
    },
) }}

SELECT
    dashboards.dashboard_id,
    dashboards.dashboard_title,
    dashboards.dashboard_url,
    dashboards.dashboard_category_id,
    CAST(dashboards_widgets.widget_id AS BIGINT),
    COALESCE(widgets.widget_title, "Text Widget") AS widget_title,
    widgets.widget_url,
    widgets_datasets.dataset_id,
    datasets.dataset_title,
    CONCAT("https://secure.holistics.io/datasets/", datasets.dataset_id) AS dataset_url,
    datasets.dataset_owner_id,
    users.user_name AS dataset_owner_name,
    users.user_email AS dataset_owner_email,
    users.user_role AS dataset_owner_role,
    widgets_datamodels.datamodel_id,
    datamodels.datamodel_title,
    CONCAT("https://secure.holistics.io/data_models?ds=8167&model", datamodels.datamodel_id) AS datamodel_url,
    sources.gbq_dataset,
    sources.gbq_table,
    sources.table_type,
    CONCAT(sources.gbq_dataset, ".", sources.gbq_table) AS full_table_name,
    mt.full_table_name IS NOT NULL AS is_manual

FROM holistics.dashboards AS dashboards
LEFT JOIN holistics.dashboards_widgets AS dashboards_widgets ON dashboards.dashboard_id = dashboards_widgets.dashboard_id
LEFT JOIN holistics.widgets AS widgets ON dashboards_widgets.widget_id = widgets.widget_id
LEFT JOIN holistics.widgets_datasets AS widgets_datasets ON dashboards_widgets.widget_id = widgets_datasets.widget_id
LEFT JOIN holistics.datasets AS datasets ON widgets_datasets.dataset_id = datasets.dataset_id
LEFT JOIN holistics.users AS users ON datasets.dataset_owner_id = users.user_id
LEFT JOIN holistics.widgets_datamodels AS widgets_datamodels ON dashboards_widgets.widget_id = widgets_datamodels.widget_id
LEFT JOIN holistics.datamodels AS datamodels ON widgets_datamodels.datamodel_id = datamodels.datamodel_id
LEFT JOIN holistics.datamodel_sources AS sources ON datamodels.datamodel_title = sources.holistics_data_model
LEFT JOIN {{ ref('manual_tables') }} AS mt ON
    mt.full_table_name = CONCAT(sources.gbq_dataset, ".", sources.gbq_table)
    AND mt.type = "bq"
