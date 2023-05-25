{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
conversion as (
    select "NoConversionAttempt" as status,
    0 as status_int
    union all 
    select "ConversionFailed" as status,
    10 as status_int
    union all 
    select "Converting" as status,
    20 as status_int
    union all 
    select "Converted" as status,
    30 as status_int
),


users AS (
  SELECT DISTINCT 
      user_id, 
      c.status as conversion_status,
      coalesce(country, "RU") as country,
      validation_status,
      reject_reason,
      owner_id,
      first_name,
      last_name
  FROM {{ ref('dim_user') }} du
  left join conversion c on du.conversion_status = c.status_int
  WHERE (not fake or fake is null)
      and next_effective_ts_msk is null
),

user_interaction as 
(select 
    _id as interaction_id, 
    date(from_unixtime(ctms/1000 + 10800)) as interaction_create_date,
    uid as user_id, 
    popupRequestID as request_id,
    description,
    friendlyId as interaction_friendly_id,
    map_from_entries(utmLabels)["utm_campaign"] as utm_campaign,
    map_from_entries(utmLabels)["utm_source"] as utm_source,
    map_from_entries(utmLabels)["utm_medium"] as utm_medium,
    source as source, 
    type as type,
    campaign as campaign,
    websiteForm as website_form,
    createdAutomatically as created_automatically,
    interactionType as interaction_type,
    conversion_status,
    country,
    validation_status,
    reject_reason,
    owner_id,
    first_name,
    last_name,
    row_number() over (partition by user_id order by interactionType, ctms) = 1 as first_interaction_type,
    row_number() over (partition by user_id, interactionType order by interactionType, ctms desc) = 1 
        as last_interaction_type
from {{ ref('scd2_interactions_snapshot') }} m
inner join users n on n.user_id = m.uid
    where dbt_valid_to is null
    and (incorrectAttribution is null or not incorrectAttribution)
),

deals as (
(
select 
    min_date as deal_start_date, 
    deal_id,
    deal_type,
    estimated_gmv,
    interaction_id,
    owner_email, 
    owner_role,
    status as deal_status,
    status_int as deal_status_int
from
{{ ref('fact_deals') }}
    where partition_date_msk = (
        select max(partition_date_msk) from {{ ref('fact_deals') }}
        )
    )
),

orders as (
   select distinct
            interaction_id,
            o.order_id,
            gmv_initial,
            initial_gross_profit,
            final_gross_profit,
            1 as orders_payed
        from {{ ref('fact_order') }} o
        left join {{ ref('gmv_by_sources') }} g on o.order_id = g.order_id
        left join user_interaction ui on o.request_id = ui.request_id
        where o.next_effective_ts_msk is null
),

order_deal as (
select 
    coalesce(d.interaction_id, o.interaction_id) as interaction_id,
    deal_start_date, 
    d.deal_id,
    deal_type,
    estimated_gmv,
    deal_status,
    deal_status_int,
    o.order_id,
    gmv_initial,
    initial_gross_profit,
    final_gross_profit,
    orders_payed
from deals d
full join orders o on d.interaction_id = o.interaction_id
left join
(select distinct deal_id, order_id
from {{ ref('fact_order_product_deal') }}
where order_id is not null
and deal_id is not null) do on d.deal_id = do.deal_id and o.order_id = do.order_id
where (d.deal_id is null or o.order_id is null or do.order_id is not null)
and coalesce(d.interaction_id, o.interaction_id) is not null
),

t1 as (
select 
    ui.interaction_id, 
    interaction_create_date,
    user_id, 
    description,
    interaction_friendly_id,
    utm_campaign,
    utm_source,
    utm_medium,
    source, 
    type as type,
    campaign,
    website_form,
    created_automatically,
    interaction_type,
    conversion_status,
    country,
    validation_status,
    reject_reason,
    owner_id,
    first_name,
    last_name,
    deal_start_date, 
    deal_id,
    deal_type,
    coalesce(estimated_gmv, 0) as estimated_gmv,
    deal_status,
    deal_status_int,
    order_id,
    coalesce(gmv_initial, 0) as gmv_initial,
    coalesce(initial_gross_profit, 0) as initial_gross_profit,
    coalesce(final_gross_profit, 0) as final_gross_profit,
    coalesce(orders_payed, 0) as orders_payed,
    first_interaction_type,
    last_interaction_type
from user_interaction ui
left join order_deal od on ui.interaction_id = od.interaction_id
),

t2 as (
    select distinct
        t2.interaction_id, 
        t2.interaction_create_date,
        t2.user_id, 
        t2.description,
        t2.interaction_friendly_id,
        t2.utm_campaign,
        t2.utm_source,
        t2.utm_medium,
        t2.source, 
        t2.type,
        t2.campaign,
        t2.website_form,
        t2.created_automatically,
        t2.interaction_type,
        conversion_status,
        country,
        validation_status,
        reject_reason,
        owner_id,
        first_name,
        last_name,
        deal_start_date, 
        deal_id,
        deal_type,
        coalesce(estimated_gmv, 0) as estimated_gmv,
        deal_status,
        deal_status_int,
        order_id,
        coalesce(gmv_initial, 0) as gmv_initial,
        coalesce(initial_gross_profit, 0) as initial_gross_profit,
        coalesce(final_gross_profit, 0) as final_gross_profit,
        coalesce(orders_payed, 0) as orders_payed,
        true as first_interaction_type,
        true as last_interaction_type
    from t1 
    left join (
        select 
            user_id, 
            interaction_id,
            interaction_create_date,
            description,
            interaction_friendly_id,
            utm_campaign,
            utm_source,
            utm_medium,
            source, 
            type,
            campaign,
            website_form,
            created_automatically,
            interaction_type
        from user_interaction
            where first_interaction_type and interaction_type in (0, 10)
    ) t2 on t1.user_id = t2.user_id
),

t3 as (
    select distinct
        t2.interaction_id, 
        t2.interaction_create_date,
        t2.user_id, 
        t2.description,
        t2.interaction_friendly_id,
        t2.utm_campaign,
        t2.utm_source,
        t2.utm_medium,
        t2.source, 
        t2.type,
        t2.campaign,
        t2.website_form,
        t2.created_automatically,
        t2.interaction_type,
        conversion_status,
        country,
        validation_status,
        reject_reason,
        owner_id,
        first_name,
        last_name,
        deal_start_date, 
        deal_id,
        deal_type,
        coalesce(estimated_gmv, 0) as estimated_gmv,
        deal_status,
        deal_status_int,
        order_id,
        coalesce(gmv_initial, 0) as gmv_initial,
        coalesce(initial_gross_profit, 0) as initial_gross_profit,
        coalesce(final_gross_profit, 0) as final_gross_profit,
        coalesce(orders_payed, 0) as orders_payed,
        true as first_interaction_type,
        true as last_interaction_type
    from t1 
    left join (
        select 
            user_id, 
            interaction_id,
            interaction_create_date,
            description,
            interaction_friendly_id,
            utm_campaign,
            utm_source,
            utm_medium,
            source, 
            type,
            campaign,
            website_form,
            created_automatically,
            interaction_type
        from user_interaction
            where last_interaction_type and interaction_type in (0, 10)
    ) t2 on t1.user_id = t2.user_id
)


select distinct *, 'all' as dim from t1
union all 
select distinct *, 'first attribution' as dim from t2
union all
select distinct *, 'last attribution' as dim from t3
