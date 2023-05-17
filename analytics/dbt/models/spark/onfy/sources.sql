
 {{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

with sources as 
(
    select distinct
        device_id,
        event_ts_cet as source_dt,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_source_dt,
        onfy_mart.device_events.type,
        coalesce(
            case 
                when onfy_mart.device_events.type = 'externalLink'
                then
                    case
                        when lower(onfy_mart.device_events.payload.params.utm_source) like '%google%' 
                        then 'google' 
                        when onfy_mart.device_events.payload.params.utm_source is not null
                        then onfy_mart.device_events.payload.params.utm_source
                        when 
                            (onfy_mart.device_events.payload.referrer like '%/www.google%') 
                            or (onfy_mart.device_events.payload.referrer like '%/www.bing%')
                            or (onfy_mart.device_events.payload.referrer like '%/search.yahoo.com%')
                            or (onfy_mart.device_events.payload.referrer like '%/duckduckgo.com%')
                        then 'Organic'
                        when
                            (onfy_mart.device_events.payload.referrer like '%facebook.com%') 
                            or (onfy_mart.device_events.payload.referrer like '%instagram.com%')             
                        then 'UNMARKED_facebook_or_instagram'      
                    end
                when onfy_mart.device_events.type in ('adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
                then
                    case
                        when onfy_mart.device_events.payload.utm_source = 'Unattributed' then 'Facebook'
                        when onfy_mart.device_events.payload.utm_source is null then 'Unknown'
                        when onfy_mart.device_events.payload.utm_source = 'Google Organic Search' then 'Organic'
                        else onfy_mart.device_events.payload.utm_source
                    end
            end
        , 'Unknown') AS utm_source,
        case 
            when onfy_mart.device_events.type = 'externalLink' 
            then onfy_mart.device_events.payload.params.utm_campaign
            else onfy_mart.device_events.payload.utm_campaign 
        end as utm_campaign,
            case 
            when 
                lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%adchampaign%'
                or lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign))like '%adchampagn%' 
                then 'adchampagne'
                when lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%rocket%' then 'rocket10'
                when lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%whiteleads%' then 'whiteleads'
                when lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%ohm%' then 'ohm'
                else 'onfy'
            end as partner
    from {{ source('onfy_mart', 'device_events') }}
    where 1=1
        and type in ('externalLink', 'adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
),


corrected_sources as 
(
    select
        sources.*,
        lower(coalesce(utm.source_corrected, sources.utm_source)) as source_corrected,
        lower(coalesce(utm.campaign_corrected, sources.utm_campaign)) as campaign_corrected
    from sources
    left join pharmacy.utm_campaigns_corrected as utm
        on coalesce(lower(utm.utm_campaign), '') = coalesce(lower(sources.utm_campaign), '') 
        and coalesce(lower(utm.utm_source), '') = coalesce(lower(sources.utm_source), '') 
)

select *
from corrected_sources
