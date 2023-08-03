{% snapshot card_bins_version %}

{{
    config(
      target_schema='payments',
      unique_key='card_bin',

      strategy='timestamp',
      updated_at='updated_ts',
      file_format='delta',
      meta = {
            'priority_weight': '150'
      }
    )
}}

select
  card_bin,
  
  card_bank,
  card_brand,
  card_country,
  card_level,
  card_type,
  
  source_bank,
  source_brand,
  source_country,
  source_type,
  
  updated_ts
  
from {{ source('payments', 'card_bins_snapshot')}}

{% endsnapshot %}
