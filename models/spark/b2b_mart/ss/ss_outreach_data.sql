{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true',
    }
) }}

with sellers_info as (
    select
        cnpj,
        legal_name,
        seller_name
    from  {{ source('joompro_mart' , 'sellers_for_outreach')}}
)

select
    cnpj,
    shop_id,
    cpf,
    contact_id,
    legal_name,
    seller_name
from {{ source('joompro_mart' , 'contacts_cpf')}}
left join sellers_info using(cnpj)
group by 1, 2, 3, 4, 5, 6
