{{ config(
    schema='onfy_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

----------------------------------------------------
-- facebook, bing & google (including partner data from OHM)
----------------------------------------------------

with fb_google_bing_base as 
(
    select
        campaign_id,
        campaign_name,
        adset_name as medium,
        'facebook' as source,
        date,
        spend,
        clicks
    from {{source('pharmacy', 'fbads_adset_country_insights')}}
    union all
    select
        campaign_id,
        campaign_name,
        null as medium,
        'google' as source,
        effective_date,
        spend,
        clicks
    from {{source('pharmacy', 'google_campaign_insights')}}
    union all 
    select 
        campaign_id,
        campaign_name,
        null as medium,
        'bing' as source,
        date,
        spend,
        clicks
    from {{source('pharmacy', 'bing_partner_insights')}}
),

----------------------------------------------------
-- calculating adchampagne: 2 * GMV 0 week
----------------------------------------------------

adchampagne_costs as 
(
    select 
        regexp_extract(campaign_name, '(\\d{16})', 1) as campaign_id,
        split(campaign_name, " ")[0] as campaign_name,
        null as medium,
        'adchampagne' as source,
        'adchampagne' as partner,
        os_type as campaign_platform,
        join_date,
        sum(revenue_W0) * 2 as spend
    from {{source('pharmacy', 'adjust_report_cohort_insights')}}
    where 1=1
        and (lower(campaign_name) like '%adc_%'
            or lower(campaign_name) like '%adcha%%')
    group by 
        campaign_name,
        campaign_platform,
        join_date
),

----------------------------------------------------
-- adding sum of clicks to adchampagne
----------------------------------------------------

adchampagne_costs_with_clicks as
(
    select 
        adchampagne_costs.campaign_id,
        adchampagne_costs.campaign_name,
        adchampagne_costs.medium,
        adchampagne_costs.source,
        adchampagne_costs.partner,
        adchampagne_costs.campaign_platform,
        adchampagne_costs.join_date,
        adchampagne_costs.spend,
        sum(clicks) as clicks
    from adchampagne_costs
    left join {{source('pharmacy', 'adjust_report_deliverables_insights')}} as adchampagne_clicks
        on adchampagne_costs.campaign_name = adchampagne_clicks.campaign_name
        and adchampagne_costs.join_date = adchampagne_clicks.join_date
        and adchampagne_costs.campaign_platform = adchampagne_clicks.os_type
    group by 
        adchampagne_costs.campaign_id,
        adchampagne_costs.campaign_name,
        adchampagne_costs.medium,
        adchampagne_costs.source,
        adchampagne_costs.partner,
        adchampagne_costs.campaign_platform,
        adchampagne_costs.join_date,
        adchampagne_costs.spend
),

----------------------------------------------------
-- define partners in facebook, google & bing
----------------------------------------------------

fb_google_bing_with_partners as 
(
    select
        campaign_id,
        campaign_name,
        medium,
        source,
        case 
           when lower(campaign_name) like '%rocket%' then 'rocket10'
           when lower(campaign_name) like '%whiteleads%' then 'whiteleads'
           when lower(campaign_name) like '%ohm%' then 'ohm'
             else 'onfy'
        end as partner,
        case when platform is not null then platform
             when platform is null and lower(campaign_name) like '%ios%' then 'ios'
             when platform is null and lower(campaign_name) like '%android%' then 'android'
             when platform is null and lower(campaign_name) like '%web%' then 'web'
             else 'other'
        end as campaign_platform,
        date as campaign_date_utc,
        spend,
        clicks
    from fb_google_bing_base
    left join pharmacy.ads_campaign_exceptions as exclusions 
      using(campaign_name)
    where 1=1
        and lower(campaign_name) not like '%adcha%'
),
 
----------------------------------------------------
-- calculate OHM comission separately (10% of ads spends)
----------------------------------------------------

ohm as 
(
    select 
        null as campaign_id,
        null as campaign_name,
        null as medium, 
        'ohm' as source,
        partner,
        campaign_platform,
        campaign_date_utc,
        sum(spend) * 0.1 as spend,
        0 as clicks
    from fb_google_bing_with_partners
    where 1=1
        and partner = 'ohm'
    group by 
        partner,
        campaign_platform,
        campaign_date_utc
),

----------------------------------------------------
-- add awin 
----------------------------------------------------

awin_costs as 
(
    select 
        null as campaign_id,
        site_name as campaign_name,
        publisher_id as medium,
        'awin' as source,
        'onfy' as partner,
        'web' as campaign_platform,
        coalesce(click_date, validation_date) as date,
        sum(coalesce(commission_amount, 0) + coalesce(network_fee, 0)) as spend,
        count(id) as clicks
    from {{source('pharmacy', 'awin_insights')}}
    where 1=1
        and commission_status = 'approved'
    group by 
        site_name,
        publisher_id,
        coalesce(click_date, validation_date)
),
              
----------------------------------------------------
-- union all of the "main" channels together
----------------------------------------------------

united_spends as
(
    select
        campaign_id,
        campaign_name,
        medium,
        source,
        partner,
        campaign_platform as campaign_platform,
        campaign_date_utc as partition_date,
        spend,
        clicks
    from fb_google_bing_with_partners
    union all
    select
        campaign_id,
        campaign_name,
        null as medium,
        case
          when source = 'adwords' then 'google'
          else source
        end as source,
        case 
          when partner = 'billiger' 
          then 'onfy' 
          else partner 
        end as partner,
        case when os_type = 'android' then os_type
             when os_type = 'ios'     then os_type
             when os_type = 'web'     then os_type
             else 'other'
        end as campaign_platform,
        join_date,
        if(spend = 0 and source = 'idealo', clicks * 0.44, spend) as spend,
        clicks
    from {{source('pharmacy', 'partners_insights')}}
    union all 
    select 
        campaign_id,
        campaign_name,
        medium,
        source,
        partner,
        campaign_platform,
        join_date,
        spend,
        clicks
    from adchampagne_costs_with_clicks
    union all 
    select
        campaign_id,
        campaign_name,
        null as medium,
        'criteo' as source,
        'onfy' as partner,
        case
            when lower(os_type) = 'ios' then 'ios'
            when lower(os_type) = 'android' then 'android'
            when lower(os_type) in ('macos', 'windows') then 'web'
            else 'other'
        end as campaign_platform,
        join_date,
        sum(spend) as spend,
        sum(clicks) as clicks
    from {{source('pharmacy', 'criteo_insights')}}
    group by 
        campaign_id,
        campaign_name,
        case
            when lower(os_type) = 'ios' then 'ios'
            when lower(os_type) = 'android' then 'android'
            when lower(os_type) in ('macos', 'windows') then 'web'
            else 'other'
        end,
        join_date
    union all
    select *
    from ohm
    union all
    select *
    from awin_costs
),

----------------------------------------------------
-- working on some "left out" partners: we're figuring out what's left in adjust tables that we previously filtered
----------------------------------------------------
     
to_filter_out as
(
    select distinct
        coalesce(cast(campaign_id as string), campaign_name, partition_date) as full_filter,
        coalesce(cast(campaign_id as string), partition_date) as partial_filter
    from united_spends
    where spend > 1
),
    
new_costs as 
(
    select 
        campaign_id,
        split(campaign_name, " ")[0] as campaign_name,
        case when 
            partner = 'adwords' then 'google'
            else partner
        end as source,
        null as medium,
        'onfy' as partner,
        os_type as app_device_type,
        os_type as campaign_platform,
        join_date,
        sum(network_cost) as cost,
        0 as clicks
    from {{ source('pharmacy', 'adjust_raw_network_insights') }}
    where 1=1
        and (
                (lower(campaign_name) not like '%adcha%')
                or (lower(campaign_name) not like '%ohm%')
                or (lower(campaign_name) not like '%mobihunter%')
                or (lower(campaign_name) not like '%s24%')
            ) 
        and 
            (
                coalesce(cast(campaign_id as string), campaign_name, join_date) not in (select distinct full_filter from to_filter_out)
            or
                coalesce(cast(campaign_id as string), join_date) not in (select distinct partial_filter from to_filter_out)
            )
    group by
        campaign_id,
        split(campaign_name, " ")[0],
        case when 
            partner = 'adwords' then 'google'
            else partner
        end,
        os_type,
        partner,
        join_date
),

----------------------------------------------------
-- spreading non-performance costs equally throughout the month
----------------------------------------------------

dates as
(
    select
        campaign_date_utc,
        campaign_month,
        add_months(campaign_month, -1) as previous_month,
        max(campaign_month) over () as max_month,
        days_in_month
    from
    (
        select distinct
            partition_date as campaign_date_utc,
            date_trunc('month', partition_date) as campaign_month,
            dayofmonth(last_day(partition_date)) as days_in_month
        from united_spends
    ) as predates
),

non_perf_spends as 
(
    select distinct
        'non-performance' as type,
        if(onfy_nonperf_spend.spend is null, 1, 0) as forecasted_spend,
        coalesce(onfy_nonperf_spend.campaign_id, forecasted_spends.campaign_id) as campaign_id,
        coalesce(onfy_nonperf_spend.campaign_name, forecasted_spends.campaign_name) as campaign_name,
        coalesce(onfy_nonperf_spend.source, forecasted_spends.source) as source,
        coalesce(onfy_nonperf_spend.medium, forecasted_spends.medium) as medium,
        coalesce(onfy_nonperf_spend.partner, forecasted_spends.partner) as partner,
        coalesce(onfy_nonperf_spend.campaign_platform, forecasted_spends.campaign_platform) as campaign_platform,
        coalesce(onfy_nonperf_spend.app_device_type, forecasted_spends.app_device_type) as app_device_type,
        cast(dates.campaign_date_utc as date) as campaign_date_utc,
        campaign_month,
        days_in_month,
        coalesce(onfy_nonperf_spend.spend, forecasted_spends.spend) / days_in_month as spend,
        0 as clicks
    from dates
    left join {{ ref("onfy_nonperf_spend") }} as onfy_nonperf_spend
        on cast(onfy_nonperf_spend.campaign_date_utc as date) = dates.campaign_month
    left join {{ ref("onfy_nonperf_spend") }} as forecasted_spends
        on cast(forecasted_spends.campaign_date_utc as date) = dates.previous_month
    where 1=1
        and coalesce(onfy_nonperf_spend.campaign_name, forecasted_spends.campaign_name) is not null
    
),

----------------------------------------------------
-- adding it all together & adding manual spends (performance & non-performance)
----------------------------------------------------

final_calculation as
(
    select
        'performance' as type,
        0 as forecasted_spend,
        campaign_id,
        campaign_name,
        source,
        medium,
        partner,
        lower(campaign_platform) as campaign_platform,
        lower(campaign_platform) as app_device_type,
        partition_date as campaign_date_utc,
        date_trunc('month', partition_date) as campaign_month,
        dayofmonth(last_day(partition_date)) as days_in_month,
        sum(spend) as spend,
        sum(clicks) as clicks
    from united_spends
    group by
        campaign_id,
        campaign_name,
        source,
        medium,
        partner,
        lower(campaign_platform),
        partition_date,
        campaign_month
    union all
    select 
        'performance' as type,
        0 as forecasted_spend,
        campaign_id,
        campaign_name,
        source,
        medium,
        partner,
        campaign_platform,
        app_device_type,
        cast(new_costs.join_date as date) as campaign_date_utc,
        date_trunc('month', new_costs.join_date) as campaign_month,
        dayofmonth(last_day(new_costs.join_date)) as days_in_month,
        cost,
        clicks
    from new_costs
    union all
    select 
        'performance' as type,
        0 as forecasted_spend,
        campaign_id,
        campaign,
        source,
        medium,
        partner, 
        campaign_platform,
        app_device_type,
        cast(onfy_manual_marketing_spends.campaign_date_utc as date) as campaign_date_utc,
        date_trunc('month', onfy_manual_marketing_spends.campaign_date_utc) as campaign_month,
        dayofmonth(last_day(onfy_manual_marketing_spends.campaign_date_utc)) as days_in_month,
        spend,
        clicks
    from {{ ref("onfy_manual_marketing_spends") }} as onfy_manual_marketing_spends
    union all
    select 
        *
    from non_perf_spends
)

select 
    type,
    max(forecasted_spend) over (partition by campaign_month) as forecasted_spend,
    campaign_id,
    campaign_name,
    source,
    medium,
    partner,
    campaign_platform,
    app_device_type,
    campaign_date_utc,
    campaign_month,
    days_in_month,
    spend,
    clicks
from final_calculation
