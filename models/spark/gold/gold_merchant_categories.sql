{{
  config(
    materialized='table',
    alias='merchant_categories',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'priority_weight': '1000',
    }
  )
}}

SELECT
    category_id AS merchant_category_id,
    name AS merchant_category_name,

    level_1_category.name AS l1_merchant_category_name,
    level_1_category.id AS l1_merchant_category_id,

    level_2_category.name AS l2_merchant_category_name,
    level_2_category.id AS l2_merchant_category_id,

    level_3_category.name AS l3_merchant_category_name,
    level_3_category.id AS l3_merchant_category_id,

    level_4_category.name AS l4_merchant_category_name,
    level_4_category.id AS l4_merchant_category_id,

    level_5_category.name AS l5_merchant_category_name,
    level_5_category.id AS l5_merchant_category_id,

    CASE
        WHEN
            level_1_category.id IN (
                '1473502934191250677-1-2-118-3896870651',
                '1571915855675353694-73-2-26202-2707303882',
                '1473502936465645123-148-2-118-982987300',
                '1473502936226769818-70-2-118-2760164976',
                '1473502943404228214-69-2-118-4165876910',
                '1473502941613522979-77-2-118-3007502750',
                '1473502937709676461-55-2-118-3614678038'
            )
            OR level_2_category.id IN (
                '1473502937919391381-124-2-118-128917952',
                '1473502938459557405-41-2-118-3447344196',
                '1473502938778845810-149-2-118-1250747924'
            )
            OR (
                level_1_category.id = '1473502940089562515-64-2-118-3246238019'
                AND level_2_category.id != '1473502945103749897-56-2-118-3819238657'
            )
            OR (
                category_id IN (
                    '1473502940089562515-64-2-118-3246238019',
                    '1499704328870930735-221-2-709-988109982',
                    '1473502937882545231-111-2-118-3567328761'
                )
            )
            THEN 'Home & Kitchen'
        WHEN
            level_1_category.id IN (
                '1473502935479416415-109-2-118-770440083',
                '1473502939349758478-83-2-118-2735028092'
            )
            THEN 'Electronics & HA'
        WHEN
            level_1_category.id IN (
                '1473502943287125397-33-2-118-4162635064',
                '1473502947431470366-33-2-118-3603304175',
                '1473502946841916116-100-2-118-270970625',
                '1473502943025251808-217-2-118-511595927',
                '1473502947544166403-72-2-118-840041103',
                '1473502946781275822-79-2-118-3985499684',
                '1473502940140554914-82-2-118-181841690'
            )
            OR level_2_category.id IN (
                '1475519040525567064-3-2-118-4293140116',
                '1474913409006796735-75-2-118-4266879428',
                '1473502945103749897-56-2-118-3819238657'
            )
            THEN 'Fashion'
        WHEN
            level_1_category.id IN (
                '1473502940600254907-239-2-118-2183002000',
                '1481193541758566184-236-2-26341-2575447955',
                '1473502937681241627-46-2-118-693451521',
                '1473502945389586288-144-2-118-2100100719',
                '1483707288087202327-155-2-629-2828156936',
                '1473502941323537435-226-2-118-934201895'
            )
            OR level_2_category.id IN (
                '1473502937885430460-112-2-118-1935676427',
                '1473502938087575017-167-2-118-1672580254'
            )
            THEN 'Health & Beauty'
        ELSE 'Other'
    END AS business_line

FROM {{ source('mart', 'category_levels') }}
