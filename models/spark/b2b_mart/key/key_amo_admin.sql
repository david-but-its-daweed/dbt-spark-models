{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with names as (
    select 'Отдел продаж' as role, 'Юлия' as name
    union all
    select 'Отдел продаж' as role, 'Ксеня Клевакина' as name
    union all
    select 'Отдел продаж' as role, 'Александр Любецкий' as name
    union all
    select 'Отдел продаж' as role, 'Юлия Толкачева' as name
    union all
    select 'Отдел продаж' as role, 'Татьяна Степаненко' as name
    union all
    select 'Отдел продаж' as role, 'Виктория Любецкая' as name
    union all
    select 'Отдел продаж' as role, 'Анна Ивкина' as name
    union all
    select 'Отдел продаж' as role, 'Дарья Королькова' as name
    union all
    select 'Отдел продаж' as role, 'Анастасия Скворцова' as name
    union all
    select 'Support' as role, 'Екатерина Ершова' as name
    union all
    select 'Support' as role, 'Анастасия Криуковтсова' as name
    union all
    select 'Support' as role, 'Андрей Поверинов' as name
    union all
    select 'Support' as role, 'Анна Кущ' as name
    union all
    select 'Support' as role, 'Галина Суслова' as name
    union all
    select 'Lead Manager' as role, 'Анна Давыдова' as name
    union all
    select 'SDR' as role, 'Анна Поливанова' as name
    union all
    select 'SDR' as role, 'Максим Шпаковский' as name
    union all
    select 'SDR' as role, 'Екатерина Жданкова' as name
    union all
    select 'SDR' as role, 'Богдан Скотарев' as name
    union all
    select 'Sales Manager' as role, 'Александр Соломин' as name
    union all
    select 'Sales Manager' as role, 'Кристина Гайко' as name
    union all
    select 'Sales Manager' as role, 'София Перекупко' as name
    union all
    select 'Sales Manager' as role, 'Евгения Бессарабова' as name
    union all
    select 'Sales Manager' as role, 'Татьяна Наследова' as name
    union all
    select 'Sales Manager' as role, 'Александр Федорцов' as name
    union all
    select 'Sales Manager' as role, 'Ольга Соломина' as name
)
    
    
select distinct
        responsibleUser.name as admin_name,
        responsibleUser.email as admin_email,
        responsibleUser._id as admin_id,
        role as admin_role
from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
join names on name = responsibleUser.name
