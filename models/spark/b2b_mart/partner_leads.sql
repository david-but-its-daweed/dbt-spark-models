{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2024-01-27', '2024-01-26', '2024-05-21', '2024-07-18', '2024-05-14']
    }
) }}

with partners as (
            select distinct partner_source, user_id as partnerId
            from {{ ref('dim_user') }}
            where partner_type = 'BigPartner'
            and next_effective_ts_msk is null
        ),

order_number as (
            select order_id, row_number() over (partition by user_id order by t) as rn
            from {{ ref('gmv_by_sources') }}
            where gmv_initial > 500
        ),

deals as (
            
            select
                user_id, 
                named_struct(
                'dealId', named_struct('oid', coalesce(dealId, '000000000000000000000000')),
                'orderID', named_struct('oid', coalesce(orderId, '000000000000000000000000')),
                'status', status,
                'statusUtms', statusUtms,
                'rejectReason', rejectReason,
                'price', price) as leadDealInfo,
                rn, 
                struct(amount, ccy) as price, date_payed as date
            from
            (
            select
                user_id, 
                case when dealId != '' then dealId end as dealId,
                orderID,
                status, statusUtms, rejectReason,
                struct(amount, ccy) as price,
                CASE WHEN rn = 1 THEN CAST(amount*0.05 as bigint) END as amount,
                ccy,
                CASE WHEN rn = 1 THEN date_payed END AS date_payed,
                rn
            from
            (
            select distinct
                dim.user_id,
                dim.deal_id as dealId,
                dim.order_id as orderId,
                case
                    when gmv_initial > 0 then 3 
                    when cast(status_int as int) > 100 or orders.status = 'cancelled'
                    then 5 else 2 end as status,
                
                cast(coalesce(
                    cast(cast(t as timestamp) as double)*1000,
                    cast(event_ts_msk as double)*1000,
                    cast(status_time as double)*1000,
                    0
                ) as bigint) as statusUtms,
                
                case
                    when cast(deals.status_int as int) > 100 then issues.status_low
                    when orders.status = 'cancelled' then orders.sub_status
                end as rejectReason,
                
                cast(coalesce(round(gmv_initial*1000000), 0) as bigint) as amount,
                cast(cast(cast(t as timestamp) as double)*1000 as bigint) as date_payed,
                'USD' as ccy,
                rn
            from (select distinct user_id, deal_id, order_id from {{ ref('dim_deal_products') }} ) dim
            left join {{ ref('fact_deals') }} as deals on deals.deal_id = dim.deal_id
                and deals.next_effective_ts_msk is null
                and deals.status is not null
            left join (select status, status_low from {{ ref('key_issue_status') }}) issues on deals.status = issues.status
            left join {{ ref('fact_order_statuses_change') }} as orders on orders.order_id = dim.order_id
                and orders.current_status
            left join {{ ref('gmv_by_sources') }} as gmv on gmv.order_id = dim.order_id
            left join order_number on gmv.order_id = order_number.order_id
            )
            where statusUtms is not null
            )
            ),
            
phone as (
            select
                lead_id,
                collect_list(phone) as phones
            from 
            (
            select distinct lead_id, phone
            from {{ ref('fact_amo_crm_contacts_phones') }}
            )
            group by lead_id
        ),

dim_user as (
            select user_id, min(effective_ts_msk) as validation_ts,
            min(created_ts_msk) as created_ts_msk,
            max(utms) as utms
            from
            (
                select 
                user_id, effective_ts_msk,
                created_ts_msk,
                case when funnel_status in ('Rejected') then 'Rejected'
                when funnel_status in ('Converting', 'ValidatedNoConversionAttempt', 'Converted', 'ConversionFailed', 'Lost') then 'Validated' else 'InProgress' end as validation_status,
                next_effective_ts_msk,
                cast(update_ts_msk as double)* (1000) as utms
            from {{ ref('dim_user') }}
            where next_effective_ts_msk is null
            )
            group by user_id
        ),

amo as (
            select oid as user_id, 
            named_struct(
                'id', named_struct('oid', coalesce(oid, '000000000000000000000000')),
                'amoLeadId', amoLeadId,
                'ctms', ctms,
                'utms', utms,
                'phones', phones,
                'validationStatus', validationStatus,
                'validationStatusUtms', validationStatusUtms) as leadInfo,
            partnerId
            from
            (
            select distinct
                user_id as oid,
                cast(amo_leads.lead_id as string) as amoLeadId,
                cast((cast(
                    coalesce(
                        amo_leads.created_ts_msk, dim_user.created_ts_msk
                    ) as double) * 1000) as bigint) as ctms,
                coalesce(
                        cast((cast(current_status_ts as double) * 1000) as bigint),
                        cast((cast(
                            coalesce(
                                amo_leads.created_ts_msk, dim_user.created_ts_msk
                            ) as double) * 1000) as bigint)) as utms,
                case when funnel_status is null or funnel_status = 'Validating' then 1
                    when funnel_status = 'Rejected' then 0 else 2 end as validationStatus,
                coalesce(cast((cast(current_status_ts as double) * 1000) as bigint),
                        cast((cast(
                        coalesce(
                            amo_leads.created_ts_msk, dim_user.created_ts_msk
                        )
                        as double) * 1000) as bigint)) as validationStatusUtms,
                phones,
                partnerId
            from {{ ref('fact_amo_crm_raw_leads') }} as amo_leads
            left join partners on lower(amo_leads.source) = lower(partners.partner_source)
            left join phone on (phone.lead_id) = (amo_leads.lead_id)
            left join dim_user using (user_id)
            where type = 'Partners' and validation
            )
            where ctms is not null
        ),


admin_phone as (
            select uid as user_id, collect_list(_id) as phones
            from 
            (
            select distinct uid, _id
            from
            {{ source('mongo', 'b2b_core_phone_credentials_daily_snapshot') }}
            )
            group by 1
        ),


admin as (
        select 
            oid as user_id,
            named_struct(
                'id', named_struct('oid', coalesce(oid, '000000000000000000000000')),
                'amoLeadId', amoLeadId,
                'ctms', ctms,
                'utms', utms,
                'phones', phones,
                'validationStatus', validationStatus,
                'validationStatusUtms', validationStatusUtms) as leadInfo,
            partnerId
            from
            (
            select distinct
                i.user_id as oid,
                '' as amoLeadId,
                cast(cast(user_created_time as double)*1000 as bigint) as ctms,
                cast(cast(utms as double) as bigint) as utms,
                case when funnel_status is null or funnel_status = 'Validating' then 1
                    when funnel_status = 'Rejected' then 0 else 2 end as validationStatus,
                cast(cast(coalesce(validated_date, validation_ts) as double) * 1000 as bigint) as validationStatusUtms,
                phones,
                partnerId
            from {{ ref('fact_attribution_interaction') }} i
            join dim_user using (user_id)
            left join admin_phone using (user_id)
            left join partners on lower(i.source) = lower(partners.partner_source)
            left join (select distinct user_id from amo) amo using (user_id)
            where type = 'Partners' and amo.user_id is null and last_interaction_type
        )
        where ctms is not null
        )


select _id.oid as partner_lead_id, * from
(select 
            named_struct(
                'oid', lower(hex(unix_timestamp(current_timestamp()))||right(md5(replace(uuid(), '-')), 16))
            ) as _id,
            cast(cast(current_timestamp() as double)*1000 as bigint) as ctms,
            coalesce(partnerId, '000000000000000000000000') as partner_id,
            named_struct('oid', coalesce(partnerId, '000000000000000000000000')) partnerId,
            leadInfo.id.oid as lead_id,
            leadInfo.amoLeadId as amo_lead_id,
            leadInfo,
            named_struct(
                'date', coalesce(date, 0), 
                'price', price
            ) as partnerPaymentInfo,
            leadDealInfo.dealId.oid as deal_id,
            leadDealInfo.orderId.oid as order_id,
            leadDealInfo,
            date('{{ var("start_date_ymd") }}') as partition_date_msk
            
from (
        select *
        from amo
        union all
        select *
        from admin
)
left join deals using (user_id)
)
