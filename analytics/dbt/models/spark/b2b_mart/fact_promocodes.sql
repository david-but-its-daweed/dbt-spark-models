{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with promocodes as (
    select 
            p._id as promocode_id,
            code,
            ownerId as owner_id,
            companyName as company_name,
            c.notes as notes,
            u.isPartner as is_partner,
            u.invitedByPromo as invited_by_promo,
            a.email,
            u._id as user_id,
            a.role
          from {{ source ('mongo', 'b2b_core_promocodes_daily_snapshot') }} p
          left join {{ source ('mongo', 'b2b_core_customers_daily_snapshot') }} c on ownerId = c._id
          left join {{ source ('mongo', 'b2b_core_users_daily_snapshot') }} u on c._id = u._id
          left join (
          select distinct user_id, email, role from {{ ref('dim_user_admin') }}
          inner join {{ ref('dim_user') }} on owner_id = admin_id
          where 1=1
          and next_effective_ts_msk is null
          ) a on ownerId = a.user_id
          where a.role != 'Dev' or u.isPartner
          order by c._id is null
          ),
          
interactions as (
    select 
        uid as user_id, 
        first_value(promocodeId) over (partition by uid order by ctms) as promocode_id,
        ctms
    from {{ source ('mongo', 'b2b_core_interactions_daily_snapshot') }}
)

select p.promocode_id,
p.code,
p.companyName as refferal_company_name,
p.notes as owner_notes,
p.is_partner,
p.user_id as refferal_id,
p.email as owner_email,
p.role as owner_role,
u.owner_id as user_owner_id,
u.company_name as user_company_name,
u.notes as user_notes,
u.email as user_admin_email,
u.user_id,
u.role as user_admin_role,
fi.interaction_id,
order_id,
created_date,
validated_date,
min_status_new_ts_msk,
min_status_selling_ts_msk,
min_price_estimation_ts_msk,
min_negotiation_ts_msk,
min_final_pricing_ts_msk,
min_signing_and_payment_ts_msk,
min_status_manufacturing_ts_msk,
current_status,
current_substatus,
final_gmv,
invited_by_promo
from promocodes p
left join promocodes u on p.promocode_id = u.invited_by_promo
left join interactions i on p.promocode_id = i.promocode_id
left join (
select 
partition_date_msk,
created_date,
validated_date,
min_status_new_ts_msk,
min_status_selling_ts_msk,
min_price_estimation_ts_msk,
min_negotiation_ts_msk,
min_final_pricing_ts_msk,
min_signing_and_payment_ts_msk,
min_status_manufacturing_ts_msk,
current_status,
current_substatus,
final_gmv,
user_id,
order_id,
interaction_id
from
b2b_mart.fact_interactions 
where rn = 1
) fi on i.user_id = fi.user_id --and date(created_date) >= date(TIMESTAMP(millis_to_ts_msk(ctms)))
