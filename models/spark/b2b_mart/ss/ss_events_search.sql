
{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


with search_events as (
Select user['userId'] AS user_id, 
        event_ts_msk, 
        type, 
        payload.pageUrl, 
        payload.source,
        payload.productId as product_id,
        payload.timeBeforeClick,
        payload.categories,
        payload.productsNumber,
        payload.page as page_num, 
        payload.query,
        payload.hotPriceProductsNumber,
        payload.topProductsNumber,
        payload.hasNextPage,
        payload.searchResultsUniqId,
        payload.page, 
        payload.index as index,
        row_number() Over(partition by payload.searchResultsUniqId, type order by event_ts_msk) as event_number 
        from {{ source('b2b_mart', 'device_events') }} 
                            where partition_date >= '2024-07-01' 
                            and type in ('productClick','search') 
                            and payload.searchResultsUniqId is not null ),
        search as (
        select 
        user_id, 
        searchResultsUniqId,
        event_ts_msk as search_ts_msk , 
        type, 
        productsNumber as product_number, 
        query
        from search_events 
        where type = 'search' 
        ),
        clicks as (
        select 
        searchResultsUniqId, 
        event_ts_msk as click_ts_msk, 
        index as position,
        product_id,
        row_number() over (partition by searchResultsUniqId order by event_ts_msk) as click_number 
        from search_events 
        where type = 'productClick' )
        
        select 
        user_id, 
        searchResultsUniqId as search_results_uniq_id,
        search_ts_msk , 
        product_number, 
        query, 
        click_ts_msk, 
        position,
        product_id,
        click_number 
        from search 
        left join clicks using(searchResultsUniqId)
