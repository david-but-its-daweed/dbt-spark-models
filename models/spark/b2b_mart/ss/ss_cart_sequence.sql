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


with subsequent as (
SELECT  
user_id, 
product_id,
sub_product_id,
event_ts_msk,
event_msk_date,
actionType, 
qty,
 lag(case when actionType in ('update_cart') then qty
          when actionType in  ('add_to_cart') then coalesce(qty,0) 

 end) over(partition by user_id,product_id,sub_product_id order by event_ts_msk ) as previus_qty,
case when actionType in ('remove_from_cart', 'move_to_deal') then -1 * qty 
when actionType in ('add_to_cart') then qty 
end change_qty

from  {{ ref('ss_events_cart') }}
ORDER BY event_ts_msk ),
subsequent_agg as (
select *, 
case when actionType in ('update_cart') then qty 
      when actionType in ('add_to_cart','remove_from_cart', 'move_to_deal' ) and 
      (coalesce(previus_qty,0) + coalesce(change_qty,0)) >= 0 then coalesce(previus_qty,0) + coalesce(change_qty,0) 
      ----else -999
      end  as current_qty
from subsequent ),
subsequent_product as (

  select 
user_id,
product_id,
 event_ts_msk,
  event_msk_date,
  current_qty,
sum(sub_qty) as sub_qty  
from (
select subsequent_agg.user_id,
      subsequent_agg.product_id,
      subsequent_agg.event_ts_msk,
      subsequent_agg.event_msk_date,
      subsequent_agg.current_qty as current_qty,
      coalesce(a.current_qty,0) as sub_qty 
from subsequent_agg 
left join (select user_id, product_id,sub_product_id, event_ts_msk as ts_hold , current_qty from  
subsequent_agg 
) a  on a.user_id = subsequent_agg.user_id 
      and a.product_id = subsequent_agg.product_id 
      and a.sub_product_id != subsequent_agg.sub_product_id 
      and a.ts_hold <= subsequent_agg.event_ts_msk
 qualify row_number() over (partition by subsequent_agg.user_id,subsequent_agg.sub_product_id,event_ts_msk,a.sub_product_id order by ts_hold desc    )  = 1 )
 group by 1,2,3,4,5
) , 
data_ as (
select user_id,
event_ts_msk,
event_msk_date, 
product_qty,
case when product_qty > 0 then 1 else 0 end as main_product,
count(distinct case when other_product_qty > 0 then other_product end) other_products,
sum(coalesce(other_product_qty,0)) as other_qty  
from ( 
select 
subsequent_product.user_id,
subsequent_product.product_id,
subsequent_product.event_ts_msk,
subsequent_product.event_msk_date,
current_qty + sub_qty  as product_qty,
b.product_id  as other_product ,
other_product_qty
from  subsequent_product
left join (
  select 
  user_id,
  product_id,
  event_ts_msk as ts_hold,
  current_qty + sub_qty  as other_product_qty
  from subsequent_product
) b on 
  subsequent_product.user_id = b.user_id 
  and subsequent_product.event_ts_msk >=  b.ts_hold 
  and subsequent_product.product_id != b.product_id 

qualify  row_number() Over(partition by 
  subsequent_product.user_id, 
  subsequent_product.product_id, subsequent_product.event_ts_msk,
   b.product_id
   order by ts_hold desc 
    )  = 1 
 )  group  by 1,2,3,4)
 
 select 
user_id,
event_ts_msk,
event_msk_date,
product_qty + other_qty as all_qty,
main_product+other_products as products 
from data_ 
order by event_ts_msk
