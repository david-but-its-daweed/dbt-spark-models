{{ config(
    schema='joompro_mart',
    materialized='view',
    meta = {
      'model_owner': '@mironov',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false'
    }
) }}

SELECT
    c.category_id,
    c.category_name,
    c.level AS level,
    c.max_level AS max_level,
    CASE
        WHEN c.level = 1 THEN NULL
        ELSE ELEMENT_AT(breadcrumb_ids, c.level - 1)
    END AS parent_id,
    ELEMENT_AT(breadcrumb_ids, 1) AS l1_id,
    ELEMENT_AT(breadcrumb_names, 1) AS l1_name,
    ELEMENT_AT(breadcrumb_ids, 2) AS l2_id,
    ELEMENT_AT(breadcrumb_names, 2) AS l2_name,
    ELEMENT_AT(breadcrumb_ids, 3) AS l3_id,
    ELEMENT_AT(breadcrumb_names, 3) AS l3_name,
    ELEMENT_AT(breadcrumb_ids, 4) AS l4_id,
    ELEMENT_AT(breadcrumb_names, 4) AS l4_name,
    ELEMENT_AT(breadcrumb_ids, 5) AS l5_id,
    ELEMENT_AT(breadcrumb_names, 5) AS l5_name,
    ELEMENT_AT(breadcrumb_ids, 6) AS l6_id,
    ELEMENT_AT(breadcrumb_names, 6) AS l6_name,
    ELEMENT_AT(breadcrumb_ids, 7) AS l7_id,
    ELEMENT_AT(breadcrumb_names, 7) AS l7_name
FROM {{ source('joompro_mart', 'mercadolibre_categories') }} AS c
WHERE
    ELEMENT_AT(breadcrumb_names, 1) NOT IN (
        'Agro',
        'Antiguidades e Coleções',
        'Carros, Motos e Outros',
        'Imóveis',
        'Ingressos',
        'Serviços'
    ) AND category_id IS NOT NULL
