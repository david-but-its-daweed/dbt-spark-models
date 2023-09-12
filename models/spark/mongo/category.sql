{{ config(
    meta = {
      'model_owner' : '@leonid.enov'
    },
    schema='mongo',
    materialized='view',
) }}
select
    _id as category_id,
    parentId as parent_id,
    createdTimeMs as created_time,
    updatedTimeMs as updated_time,
    name,
    public,
    googleId as google_id,
    sizeTableInfo.required as size_table_required
from {{ source('mongo', 'core_categories_daily_snapshot') }}