SELECT 
    category_id,
    name,
    level_1_category.name AS l1_category_name,
    level_1_category.id AS l1_category_id,

    level_2_category.name AS l2_category_name,
    level_2_category.id AS l2_category_id,

    level_3_category.name AS l3_category_name,
    level_3_category.id AS l3_category_id,

    level_4_category.name AS l4_category_name,
    level_4_category.id AS l4_category_id,

    level_5_category.name AS l5_category_name,
    level_5_category.id AS l5_category_id

FROM {{ source('mart', 'category_levels') }}