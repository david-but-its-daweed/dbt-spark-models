{% snapshot referral_bloggers_info %}

{{
    config(
      target_schema='customer_routing',
      unique_key='user_id',

      strategy='check',
      check_cols='all',
      file_format='delta',
      invalidate_hard_deletes=True
    )
}}

SELECT 
    user_id AS user_id,
    email AS email,
    language AS language,
    user_name AS user_name,
    created_at AS created_at

FROM {{ source('ads', 'referral_bloggers') }}
{% endsnapshot %}