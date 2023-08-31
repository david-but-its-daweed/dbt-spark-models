# Таблицы без расхождений (полное совпадение кол-ва записей и значений):
- models.dim_pair_currency_rate
- models.card_bins
- models.categories
- models.payment
- gold.regions
- gold.countries
- gold.payment_orders
- gold.logisitics_orders
- gold.referral_orders
- gold.merchant_categories

# Таблицы с рахождениями
Ниже "небольшое" расхождение означает < 1% строк, "ок" - означает, что точно сошлось.
Расхождение в данных считается, если есть хоть одно поле, значение которого отличается в Spark и BQ


## gold.merchants
Кол-во записей - ок.

Данные - расходится updated_datetime_utc из-зв того, что в spark не выполняется `date_truncate(.., second)`, и значение хранится с милисекундами.


## models.active_users
Кол-во записей - небольшое расхождение, потому что не все данные представлены в BQ.

Данные - небольшое расхождение из-за недетерминированного any_value, поля:
- country
- platform
- os_version
- app_version

## models.active_devices
То же, что и с models.active_users

## models.gold__active_devices_with_ephemeral
То же, что и с models.active_devices + lag/lead по окну, отсортированному по date_msk

## models.gold__active_users_with_ephemeral
То же, что и с models.gold__active_devices_with_ephemeral 


## models.joom_babylone_tickets
Кол-во записей - ок.

Данные - расхождение (порядка 2%) по полям, которые тянутся из active_users. Поля:
- country
- platform


## models.bloggers
Кол-во записей - небольшое расхождение.

Данные - ок.

## models.orders
Кол-во записей - небольшое расхождение в истории из-за расползания BQ и Spark

Данные - расхождения в округляемых полях, потому что в BQ работает округление не так, как в спарке.

Примеры с округлением:
```sql
select round(0.6375, 3); -- дает 0.637 в bq, в спарке - 0.638
```
Пример на реальных данных (BQ):
```sql
select
  order_id,
  merchant_revenue_final,
  round(merchant_revenue_final, 3)
from `mart.star_order_2020`
where partition_date = "2023-08-20" and order_id = "64e27c5b15b7b538682ce6eb"
```

Есть расхождения в поле product_order_number из-за недетерминированного row_number() по окну с сортировкой по полю с одинаковыми значениями. 

Пример (spark) 
```sql
select
    order_id,
    product_id,
    created_time_utc, -- должны быть записи с одинаковым created_time_utc 
    partition_date

from mart.star_order_2020
where  product_id = '607d869119f803012e27f6b1' and partition_date in ("2023-08-20", "2023-01-15")
limit 10
```

## gold.order_groups
Кол-во записей - ок.

Расхождения в данных >1% по кол-ву записей.

Заафекченные поля:
- gmv_initial_in_local_currency
- ecgp_initial
- merchant_revenue_initial
- merchant_revenue_final
- merchant_sale_price
- logistics_price_initial
- marketplace_commission_initial
- coupon_discount

Причина - кумулятивная ошибка по полям с округлением (см. models.orders)


## gold.orders
Кол-во данных - ок.

Существенное расхождение (порядка 25% строк) по данным в полях:
- product_orders_number
- device_orders_number
- user_orders_number
- real_user_orders_number

Причина - недетерминированный row_number() over (... order by created_time_utc).
Запрос для проверки:
```sql
select 
        order_id,
        device_id,
        created_time_utc, -- должен повторяться
        partition_date 
from mart.star_order_2020 
where device_id="634c33afea8365337a457351" 
order by created_time_utc
```

## models.payment_order
Количество записей - ок.

Существенное расхождение по данным (порядка 55%).

Из них 50% - это поля
- number_att_provider
- number_att_pmt_type
 
Причина: недермнированный row_number() по окну без сортировки. В спарке нельзя делать row_number без сортировки, поэому добавид сортировку по created_time

Еще 5% - поля
- category_id
- category_name

Причина: недерминированный first_value() по окну с сортировкой по полю с одинковыми значениями.

Пример (BQ):
```sql

select
   order_group_id,
   gmv_initial,  -- должны быть одинаковые gmv_initial
   category_id
from models.orders
where day = "2023-08-20" and  category_id in ("1473502941040270137-131-2-118-1881693528", "1481899759819871582-210-2-26341-1721687645") and order_group_id = "64e1437715b7b53768f2e1c9"
order by gmv_initial;
```

## gold.products
Кол-во данных - ок

По данным - небольшое расхождение в поле
* current_merchant_sale_price

Причина - last_value по окну с сортировкой по полю order_datetime_utc. Значения этого поля могут повторяться.
