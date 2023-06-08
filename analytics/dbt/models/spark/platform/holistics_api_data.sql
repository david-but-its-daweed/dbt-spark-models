{{ config(
    schema='holistics',
    materialized='view'
    meta = {
      'bigquery_load': 'true'
    },
) }}

SELECT
    dashboards.dashboard_id,
    dashboards.dashboard_title,
    dashboards.dashboard_url,
    dashboards_widgets.widget_id,
    COALESCE(widgets.widget_title, "Text Widget") AS widget_title,
    widgets.widget_url,
    widgets_datasets.dataset_id,
    datasets.dataset_title,
    CONCAT('https://secure.holistics.io/datasets/',datasets.dataset_id) AS dataset_url,
    datasets.dataset_owner_id,
    users.user_name AS dataset_owner_name,
    users.user_email AS dataset_owner_email,
    users.user_role AS dataset_owner_role,
    widgets_datamodels.datamodel_id,
    datamodels.datamodel_title,
    CONCAT('https://secure.holistics.io/data_models?ds=8167&model', datamodels.datamodel_id) AS datamodel_url,
    sources.gbq_dataset AS gbq_dataset,
    sources.gbq_table AS gbq_table,
    sources.table_type as table_type
FROM holistics.dashboards AS dashboards
         LEFT JOIN holistics.dashboards_widgets AS dashboards_widgets ON dashboards_widgets.dashboard_id = dashboards.dashboard_id
         LEFT JOIN holistics.widgets AS widgets ON widgets.widget_id = dashboards_widgets.widget_id
         LEFT JOIN holistics.widgets_datasets AS widgets_datasets ON widgets_datasets.widget_id = dashboards_widgets.widget_id
         LEFT JOIN holistics.datasets AS datasets ON datasets.dataset_id = widgets_datasets.dataset_id
         LEFT JOIN holistics.users AS users ON users.user_id = datasets.dataset_owner_id
         LEFT JOIN holistics.widgets_datamodels AS widgets_datamodels ON widgets_datamodels.widget_id = dashboards_widgets.widget_id
         LEFT JOIN holistics.datamodels AS datamodels ON datamodels.datamodel_id = widgets_datamodels.datamodel_id
         LEFT JOIN holistics.datamodel_sources AS sources ON sources.holistics_data_model = datamodels.datamodel_title
