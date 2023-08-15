SELECT 
  category_id,
  name,
  level_1_category.name as l1_category_name,
  level_1_category.id as l1_category_id,

  level_2_category.name as l2_category_name,
  level_2_category.id as l2_category_id,

  level_3_category.name as l3_category_name,
  level_3_category.id as l3_category_id,

  level_4_category.name as l4_category_name,
  level_4_category.id as l4_category_id,

  level_5_category.name as l5_category_name,
  level_5_category.id as l5_category_id

FROM {{source('mart', 'category_levels')}}