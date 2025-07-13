{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@amitiushkina',
      'priority_weight': '150',
      'bigquery_load': 'true'
    }
) }}

WITH with_row_id AS (
    SELECT
        *,
        MONOTONICALLY_INCREASING_ID() AS row_id
    FROM {{ ref('joom_pro_manual_orders') }}
),

latest_values AS (
    SELECT
        order_id,
        FIRST(order_created_date, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_order_created_date,
        FIRST(gmv_initial, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_gmv_initial,
        FIRST(initial_gross_profit, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_initial_gross_profit,
        FIRST(final_gross_profit, TRUE) OVER (PARTITION BY order_id ORDER BY row_id DESC) AS last_final_gross_profit
    FROM with_row_id
),

final_manual AS (
    SELECT DISTINCT
        order_id,
        last_order_created_date AS order_created_date,
        last_gmv_initial AS gmv_initial,
        last_initial_gross_profit AS initial_gross_profit,
        last_final_gross_profit AS final_gross_profit
    FROM latest_values
)

SELECT
    COALESCE(man.order_created_date, ord.t) AS t,
    ord.order_id,
    COALESCE(man.gmv_initial, ord.gmv_initial) AS gmv_initial,
    COALESCE(man.initial_gross_profit, ord.initial_gross_profit) AS initial_gross_profit,
    COALESCE(man.final_gross_profit, ord.final_gross_profit) AS final_gross_profit,
    ord.utm_campaign,
    ord.utm_source,
    ord.utm_medium,
    ord.source,
    ord.type,
    ord.campaign,
    ord.retention,
    ord.user_id,
    ord.country,
    ord.owner_email,
    ord.owner_role,
    ord.first_order,
    ord.client,
    ord.current_client
FROM {{ ref('gmv_by_sources_wo_filters') }} AS ord
LEFT JOIN final_manual AS man USING (order_id)
WHERE ord.order_id NOT IN (
    '657c58febbbdb8729dd7d39e', '658d3fc317e10341173c1f20', '659d3c4dddc19670cf999989',
    '65aa2a7ff11499f63900def9',
    '643d72b2acb1823e59eab7c5',
    '65e9c5b30d65cd6752992149',
    '663b87b734a8066e53968a66',
    '665f0ae08a8dac1a3ee66e1d',
    '666374d84c64a4aeb7c9aecb',
    '669ac6f8c980840fd9e5f2b7',
    '66a7aab79aa8c1361243e9d2', '66b388e4c5ad160d2d8de692', '66b3b9bc877fd3da349bb8ef',
    '66b1223b87628c73dce32443', '66c4f62e0b5bb457ba720a4f', '66ccad998d98f755caa68ad9',
    '66d20c48329656118385ec5a', '66d5f6bcace18af6ab5d4860', '66ce11498d98f755caa6923b',
    '66d9c9a1b1fcc25b92ce3f1d', '66da0c24182675e7411f2a28', '66e2f6c283d6709f08a84b03',
    '66ed74676f09959ab0fd0f12', '66ec20d252035764e906295a', '66f300b4e1ed52479981bd57',
    '66f5a143210729de9b82999e', '66fd455b0052353d1ce86fc3', '66fd5b223beac4b744f2c699',
    '670fa15a5b651d83f456bd54', '67090fcf4348d72dc52e7e89', '67100ba05b651d83f456c2bb',
    '671be7a60c96a194287a2766', '6720dff3ea371c645df9b48f', '67212806ea371c645df9b5a5',
    '67f041914cd4c9c1e3af160a',
    '68022baae68e18d5593daa01',
    '68022185bc59189378b6d45c',
    '680242f0f18d2b8e7c6ff5fc',
    '6808ce5a5ec4efc9e2cd516b',
    '680a29b3409b3532064ace45',
    '680b3fb429ef6c2a23417e4c',
    '680f6d22b38fac4193897d59',
    '680bd037181e191aa27c8c2e',
    '681259229b105dfe6f3d42ef',
    '680bd18da38a8e90ba844478',
    '68152bab78e922306f2b8fee',
    '682182a26244d4534f6513ab',
    '681922a3696bd482ea3a981d',
    '68186fe8f4c56c2d31908d91',
    '681de3a6b8ab5f9bd56931a7',
    '682494ae9a0f0f6479a3bd80',
    '682b85cc204b6aadb4660ce9',
    '682c32d1188bacf0fd4cfe5e',
    '684045673322b884d25229e9',
    '683067646072d4ba1d9148c6',
    '68394d4afc4d57741a7c448f',
    '6838d18c17805cbf42b99c58',
    '683957963570061309821f23',
    '68394dbe357006130979e112',
    '683962dc19bdaaac832ef367',
    '68498f06380f8bc708692bcd',
    '68402937ad09c7951d1c4d53',
    '683d59cd3570061309dac293',
    '68400148382e75f456c95554',
    '683df70ff402f389e80e15e5',
    '684030d8382e75f456d6e898',
    '68414a7d690f00a8e43def02',
    '6849763ad936404283d224ed',
    '6846e303223c457ec0377a33',
    '6841ed0d127c422b7471a161',
    '6841fa54127c422b7471a1b7',
    '6841f232b4d67eca1cd65b3b',
    '685036b9760d5b888da66846',
    '68503742627698784adb4d3b',
    '6849741e38037bcf0a2627d9',
    '685044b2be543f9ddbf1d84a',
    '68498377c8d5614e51c1d80c',
    '685043c10b16db71401b829f',
    '6855324adfcd3c00323d7f4e',
    '68511c843d7c3c7d671aa2ca',
    '6851231093c9b96ac3ee12e1',
    '6852abbd22cf7ca9db034f97',
    '6851a535c205edb8e9f16775',
    '685532bfb85864c455b83fe4',
    '685531883ce5c5aa6991e120',
    '6855306ff770203684bb1da9',
    '685305f881fd290f73c1e3d4',
    '6859a734aecb56d3a3e6f31b',
    '6859a1557c220eea27cc8a1e',
    '685e3b2dcf827ae29cd40b26',
    '685d84a1890e8dc3510b6c06',
    '685d86e495b08a50174d7b9b',
    '685e471541ac63cd7a334530',
    '685c0b86d584cc4a3e6537bf',
    '685e4b1628837e3bda1f551d',
    '685d991900ed0c6ddb26ae41'
)
