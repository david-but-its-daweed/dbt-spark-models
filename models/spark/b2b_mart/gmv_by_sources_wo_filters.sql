{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'priority_weight': '150',
      'bigquery_load': 'true'
    }
) }}



WITH not_jp_users AS (
    SELECT DISTINCT
        user_id,
        owner_id AS owner_moderator_id
    FROM {{ ref('dim_user') }}
    WHERE
        (NOT fake OR fake IS NULL)
        AND next_effective_ts_msk IS NULL
),

admin AS (
    SELECT
        admin_id,
        a.email,
        a.role AS owner_role
    FROM {{ ref('dim_user_admin') }} AS a
),

order_v2_mongo AS (
    SELECT
        fo.order_id AS order_id,
        fo.user_id,
        DATE(fo.created_ts_msk) AS created_ts_msk,
        DATE(fo.min_manufactured_ts_msk) AS manufactured_date,
        MIN(DATE(fo.min_manufactured_ts_msk)) OVER (PARTITION BY fo.user_id) AS min_manufactured_date,
        u.owner_moderator_id
    FROM {{ ref('fact_order') }} AS fo
    INNER JOIN not_jp_users AS u ON fo.user_id = u.user_id
    WHERE (order_id in ('6735ffb2f182e3f4a318c0ee',
                        '673b8dd150f7107c6a2ee8f0',
                        '675af7b808433ed244d897e9',
                        '675c67042e1d04dfe942898d',
                        '6759f0a917c167b3cf1d5bad',
                        '675b02d4781d3609c1e97225',
                        '675c7411efa9f41236d87439',
                        '675c6ea3efa9f41236d87427',
                        '675c462f3703512d44aaa91b',
                        '675c3d6b3703512d44aaa8e0',
                        '675c69e52e1d04dfe94289c5',
                        '67606db7f7660794452fa237',
                        '6762d82cf7660794452faf52',
                         '6765968e7c2acf46df9a3a82',
                        '677e7b72dfbe6dd93320672e',
                         '677eafa37104bc79f2a8bb6c',
                         '677eb6a4dfbe6dd933206831',
                         '677edff8dfbe6dd9332068d6',
                         '677ed4487104bc79f2a8bc04',
                         '677ee0d67104bc79f2a8bc29',
                         '677fd23a7104bc79f2a8c055',
                         '677fe6597104bc79f2a8c0e8',
                         '6761acb706c420992f837ae0',
                        '6776e4f7dfbe6dd933205c82',
                        '678156514f74fe5dab4f0c6c',
                        '6784f1bd7294db47e074844d',
                        '6781672b4f74fe5dab4f0cbc',
                        '67815ca154880f4bf013c533',
                        '6784eb1af7e293bf7267b519',
                        '678567232583002a9a42efa2',
                        '6785603c5362eed40410d47c',
                        '67855e342583002a9a42ef84',
                        '67868f632583002a9a42f5f6',
                        '67865e4c5362eed40410d955',
                        '678692cc2583002a9a42f608',
                        '67866bd05362eed40410d9b9',
                        '6788203e2583002a9a42fc1e',
                        '6787d94b5362eed40410df41',
                        '678805352583002a9a42fb85',
                        '6788e90a5362eed40410e435',
                        '6789660f46dd05f18e97ab1b',
                        '67897626e4dd3a64d0d31119',
                        '678970a2e4dd3a64d0d310ff',
                        '678abd0e7958f9ced43ba45e',
                        '678ac3e67958f9ced43ba483',
                        '678aaf15e4dd3a64d0d3193f',
                        '6740d51a4bd146113e694424',
                        '678ecc739ff44121e2a35092',
                        '678ea4029ff44121e2a35049',
                        '678e95ff1f6966d82bcdbe0b',
                        '678e9a421f6966d82bcdbe23',
                        '678ee94d1f6966d82bcdbe76',
                        '678ee6761f6966d82bcdbe64',
                        '678f8dc79ff44121e2a354bf',
                        '678ff923cbf322f5bc3cc40f',
                        '67900498cbf322f5bc3cc460',
                        '678ffe6c13aa958d1187ca32',
                        '678ffd1d13aa958d1187ca1f',
                        '678ffa1fcbf322f5bc3cc424',
                        '6790141513aa958d1187cae5',
                        '679106d213aa958d1187d734',
                        '679144d20d845cb3bb23d5b8',
                        '6787ffe82583002a9a42fb69',
                        '67913fc5ffc70babfdf2eb4b',
                        '67913bf5ffc70babfdf2eb2e',
                        '679156a20d845cb3bb23d5fa',
                        '6790e16ccbf322f5bc3cc7be',
                        '679159930d845cb3bb23d62b',
                        '679275267082c0897435a8b2',
                        '67927160fa4fd98c7be79ae5',
                        '679287497082c0897435b156',
                        '679270117082c0897435a88e',
                        '679272c8fa4fd98c7be79af4',
                        '679275487082c0897435a8be',
                        '679277a6fa4fd98c7be79b22',
                        '67928a327082c0897435b18e',
                        '67929b1d2866b481b2a24d93',
                        '6792d97fc36f2c7f811dc342',
                        '6792ca7cfd448e40fe03397a',
                        '6792d68cfd448e40fe0339cf',
                        '6793ca0b537d8fbd6e3bd8ce',
                        '6793a0ea21f03f0a4079393a',
                        '6793df68d253e1d017ad9723',
                        '679411acd253e1d017ad99b7',
                        '6797885eb00d9e8df2652347',
                        '6793cc7e537d8fbd6e3bd8f5',
                        '6793e15ed253e1d017ad974b',
                        '679414d1d6acb53543fe5d4b',
                        '6793d988d6acb53543fe5a3e',
                        '6793e207d6acb53543fe5add',
                        '679b7ba16fe9614353c826d8',
                        '67975eb7b00d9e8df265210a',
                        '6797e073e1842903112967a7',
                        '6797ab55cc18a9df8ad57707',
                        '6797d0bbcc18a9df8ad5774e',
                        '6797a24dcc18a9df8ad576a1',
                        '6797f63dcc18a9df8ad577c1',
                        '6797dbbce184290311296798',
                        '6798d32085fa3855c0b56eee',
                        '6798e66785fa3855c0b5708f',
                        '6798dae985fa3855c0b56fbf',
                        '6798dc0c3e283bd9790a77ea',
                        '679929d385fa3855c0b5740f',
                        '679925b83e283bd9790a7b8b',
                        '67992de23e283bd9790a7bff',
                        '679a3b075760b420efa32f75',
                        '679a97c050edc4f7f8a21d71',
                        '679a2ac711a5940bca86ab0c',
                        '679a8be56fe9614353c8244f',
                        '679b809850edc4f7f8a21f59',
                        '679b9f5963be474e81bdc62c',
                        '679bb58763be474e81bdc75c',
                        '679bc9a163be474e81bdc883',
                        '679b9a9761c09318cde7c014',
                        '679cdc7661c09318cde7c840',
                        '679cd7df63be474e81bdce1e',
                        '679b9d0e61c09318cde7c035',
                        '679d373163be474e81be1fe7',
                        '679d2b1861c09318cde8191f',
                        '679d258763be474e81be1ee7',
                        '679d2c6e63be474e81be1f56',
                        '679d3b6561c09318cde819ea',
                        '67a10f1a72b672afc4f47b91',
                        '67a11ae562f5db5e83f0ced7',
                        '67a0ff6909fec9cbdd45b05b',
                        '67a10ea072b672afc4f47b85',
                        '67a11bc162f5db5e83f0cee5',
                        '67a12a1762f5db5e83f0f2e2',
                        '67a2786f72b672afc4f4b124',
                        '67a25cb262f5db5e83f0fc53',
                        '67a2315672b672afc4f4b05f',
                        '67a257f672b672afc4f4b0ba',
                        '67a2418362f5db5e83f0fc0e',
                        '67a38493e5e4959a3b39f0f1',
                        '67a3aa15950775cbf83f5a77',
                        '67a39736e5e4959a3b39f131',
                        '67a38149e5e4959a3b39f0cb',
                        '67a3ac0d950775cbf83f5a8b',
                        '67a4a81d54470e5eb2c34048',
                        '67a503b1fefad648d1bea592',
                        '67a50ea9ac4136f0de8615fa',
                        '67a50e37ac4136f0de8615f2',
                        '67a66860ad358c9c8df75052',
                        '67a661d1a0df8d70aee629b3',

'6797e073e1842903112967a7',
'67992de23e283bd9790a7bff',
'679a8be56fe9614353c8244f',
'679bb58763be474e81bdc75c',
'679bc9a163be474e81bdc883',
'67a3aa15950775cbf83f5a77',
'67a3ac0d950775cbf83f5a8b',
'67a9f0f7a0df8d70aee62d46',
'67a50e37ac4136f0de8615f2',
'67a66860ad358c9c8df75052',
'67aa44af47d717642ccf3d76',
'67aa3fd147d717642ccf3d61',
'67aa53dfe7e15b582830f22f',
'67aa562ce7e15b582830f249',
'678582a52583002a9a42f01a',
'67aa5dec51715bab47e68682',
'67aa54a951715bab47e68664',
'67aa5efd51715bab47e68690',
'67aa4b5e47d717642ccf41ca',
'67aa4ff1e7e15b582830f223',
'67aa2734a0df8d70aee62e6e',
'67aa4811bcd42fa342691cd4',
'67aaaf6851715bab47e6a0d1',
'67ab48fce7e15b582831325c',
'67ab500c51715bab47e6a7ec',
'67ab57ace7e15b58283132b8',
'67ab5d70e7e15b5828313303',
'67aa8ea751715bab47e69f71',
'67ab5f4551715bab47e6a843',
'67abaaf1e7e15b58283133bf',
'67ab5923e7e15b58283132db',
'67af4f32ab7537262edf2d66',
'67abe5f751715bab47e6a949',
'67ae340471e3ebc3ad12b011',
'67ace8afa69847ef8ee85e51',
'67ad000ca69847ef8ee86f1c',
'67ae328671e3ebc3ad12affe',
'67ae383a71e3ebc3ad12b01c',
'67af41a0ab7537262edf2cfe',
'67ae557371e3ebc3ad12b069',
'67af4ed6ab7537262edf2d53',
'67ae365b618b75db93159c28',
'67af8a318b71e3a0bc3687e8',
'67adfe2f71e3ebc3ad12af23',
'67af8174ab7537262edf2df5',
'67afc63b8b71e3a0bc368846',
'67af82e78b71e3a0bc3687c8',
'67af787cab7537262edf2ddc',
'67af7355ab7537262edf2dc7',
'67af9879ab7537262edf2e60',
'67af986bab7537262edf2e54',
'67af8585ab7537262edf2e08',
'67af9882ab7537262edf2e6d',
'67af9893ab7537262edf2e79',
'67adf2b871e3ebc3ad12ae49',
'67b3c1325993703ae967e8ee',
'67b36e2f5993703ae967e807',
'67b3b5b39a88dfc5b34c9276',
'67b3c7f85993703ae967e8fe',
'67b3ab619a88dfc5b34c9258',
'67b39ec25993703ae967e8ae',
'67b6344cc42752d15569963d',
'67b484059a88dfc5b34c95b9',
'67b4df6fa01a855e7f358884',
'67b4ef69a01a855e7f35896f',
'67b4f7001050f68df2141ebf',
'67b4f9f31050f68df2141efa',
'67b4f8bba01a855e7f358a13',
'67b4cc0da01a855e7f35876d',
'67b4f2da1050f68df2141e76',
'67b4ca56a01a855e7f35874c',
'67b7c7212797a9eb03d567db',
'67b4916cd9b38f7cfe2bc2c3',
'67b4f0df1050f68df2141e50',
'67b4b09b401f855a2cd97af0',
'67b65b3f236512cfec8e05f0',
'67b67cc3236512cfec8e061f',
'67b8ac5b12b9c436847b872a',
'67b636a2c42752d15569966d',
'67b6359bc42752d15569965c',
'67b5ce7d38476941472b96ee',
'67b634dbc42752d155699647',
'67b63d64236512cfec8e05a9',
'67b637e9c42752d15569967d',
'67b795b3c8aeeff56e1e0613',
'67b6372e236512cfec8e0583',
'67b634fb236512cfec8e0559',
'67b7da7f2797a9eb03d567f0',
'67b774602797a9eb03d56739',
'67b8b8ee0e3991c09fa501e2',
'67b78e2a2797a9eb03d56778',
'67b7e7d2c8aeeff56e1e0693',
'67b7debc2797a9eb03d567fe',
'67b7af422797a9eb03d567b7',
'67b780892797a9eb03d56758',
'67b7d873c8aeeff56e1e0674',
'67b7e2c22797a9eb03d5680f',
'67b7d628c8aeeff56e1e0663',
'67b8f37112b9c436847b87f1',
'67b8b60a12b9c436847b8749',
'67b8ba900e3991c09fa501f5',
'67b8ab430e3991c09fa501b8',
'67b8e0ae0e3991c09fa50280',
'67b8de2f0e3991c09fa50268',
'67b8d9540e3991c09fa5023d',
'67b8d84d0e3991c09fa5022c',
    '67bc894f6625fbade5ded864',
'67bf7a4323ab3402f6fdc31e',
'67bbb87712b9c436847bd185',
'67bbbc160e3991c09fa573cd',
'67bbb9fd0e3991c09fa573c0',
'67be492efa83fa73798bfaa6',
'67bccc244c87410d3c891e56',
'67bdb0bf6625fbade5dedd32',
'67bcca546625fbade5ded9c6',
'67bdb01a6625fbade5dedd19',
'67bcc8ee6625fbade5ded9ac',
'67bcd0a04c87410d3c891e6b',
'67bc99226625fbade5ded8cd',
'67bc950a6625fbade5ded8b3',
'67bc9e746625fbade5ded8ed',
'67bdc5f7e6b1bb4154559784',
'67bf67f6b8ef5126941af9cb',
'67bd07ec4c87410d3c891f0f',
'67be23e1d4c7ca98e6aff179',
'67be4dfdd4c7ca98e6aff1c7',
'67be4bcefa83fa73798bfab5',
'67be07b3d4c7ca98e6aff0ee',
'67be1855d4c7ca98e6aff135',
'67bdc6cbc754ab13a73a54b4',
'67be19e3d4c7ca98e6aff146',
'67bf0f33fa83fa73798bfe2c',
'67be3837d4c7ca98e6aff1a5',
'67be20d5fa83fa73798bfa46',
'67be0c40fa83fa73798bf9fc',
'67be4ef2d4c7ca98e6aff1d8',
'67be5071d4c7ca98e6aff1e8',
'67be4810fa83fa73798bfa99',
'67bf930d23ab3402f6fdc371',
'67c04f12b8ef5126941afd35',
'67c0cdb94c655ff8a056e1eb',
'67bf84d323ab3402f6fdc341',
'67bca8eb4c87410d3c891db7',
'67c213f4123d7dbb33dc1351',
'67c1f4c2123d7dbb33dc12dd',
'67c211ac9465c86bf826d266',
'67bfda7723ab3402f6fdc3e8'
        
    ) AND fo.next_effective_ts_msk IS NULL) or 
        (fo.last_order_status < 60
        AND fo.last_order_status >= 10
        AND fo.next_effective_ts_msk IS NULL
        AND fo.min_manufactured_ts_msk IS NOT NULL)
),

order_v2 AS (
    SELECT
        order_id,
        total_confirmed_price,
        final_gross_profit,
        initial_gross_profit,
        owner_moderator_id
    FROM (
        SELECT
            order_id,
            total_confirmed_price,
            final_gross_profit,
            initial_gross_profit,
            owner_moderator_id,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts_msk DESC) AS rn
        FROM {{ ref('fact_order_change') }}
    )
    WHERE rn = 1
),

source AS (
    SELECT
        user_id,
        source,
        type,
        campaign,
        utm_campaign,
        utm_source,
        utm_medium,
        min_date_payed
    FROM {{ ref('fact_attribution_interaction') }}
    WHERE last_interaction_type
),

after_second_qrt_new_order AS (
    SELECT
        o.manufactured_date AS t,
        p.order_id,
        SUM(p.total_confirmed_price) AS gmv_initial,
        SUM(p.initial_gross_profit) AS initial_gross_profit,
        SUM(p.final_gross_profit) AS final_gross_profit,
        s.utm_campaign,
        s.utm_source,
        s.utm_medium,
        s.source,
        s.type,
        s.campaign,
        o.user_id,
        a.email AS owner_email,
        a.owner_role,
        COALESCE(NOT(s.min_date_payed IS NULL OR DATE(s.min_date_payed) >= o.created_ts_msk), TRUE) AS retention,
        CASE
            WHEN o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
        END AS first_order
    FROM order_v2 AS p
    INNER JOIN order_v2_mongo AS o ON p.order_id = o.order_id
    LEFT JOIN source AS s ON o.user_id = s.user_id
    LEFT JOIN admin AS a ON o.owner_moderator_id = a.admin_id
    WHERE p.order_id NOT IN ('6294f3dd4c428b23cd6f2547', '64466aad3519d01068153f0b')
    GROUP BY
        o.manufactured_date,
        p.order_id,
        s.utm_campaign,
        s.utm_source,
        s.utm_medium,
        s.source,
        s.type,
        s.campaign,
        COALESCE(NOT(s.min_date_payed IS NULL OR DATE(s.min_date_payed) >= o.created_ts_msk), TRUE),
        CASE
            WHEN o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
        END,
        o.user_id,
        a.email,
        a.owner_role
),

country AS (
    SELECT DISTINCT
        user_id,
        COALESCE(country, "RU") AS country
    FROM {{ ref('dim_user') }}
    WHERE next_effective_ts_msk IS NULL
),

users AS (
    SELECT DISTINCT
        user_id,
        day,
        client
    FROM (
        SELECT
            a.user_id,
            d.day,
            CASE
                WHEN
                    SUM(CASE WHEN a.t > add_months(d.day, -6) AND a.t <= d.day THEN a.gmv_initial ELSE 0 END)
                        OVER (PARTITION BY a.user_id, d.day) > 100000 THEN 'big client'
                WHEN
                    SUM(CASE WHEN a.t > add_months(d.day, -6) AND a.t <= d.day THEN a.gmv_initial ELSE 0 END)
                        OVER (PARTITION BY a.user_id, d.day) > 30000 THEN 'medium client'
                ELSE 'small client'
            END AS client
        FROM (
            SELECT DISTINCT
                order_id,
                user_id,
                gmv_initial,
                t,
                1 AS for_join
            FROM after_second_qrt_new_order
        ) AS a
        LEFT JOIN (
            SELECT
                EXPLODE(SEQUENCE(to_date('2022-03-01'), to_date(CURRENT_DATE()), INTERVAL 1 DAY)) AS day,
                1 AS for_join
        ) AS d ON a.for_join = d.for_join
    )
)

SELECT
    a.t,
    a.order_id,
    a.gmv_initial,
    a.initial_gross_profit,
    a.final_gross_profit,
    a.utm_campaign,
    a.utm_source,
    a.utm_medium,
    a.source,
    a.type,
    a.campaign,
    a.retention,
    a.user_id,
    c.country,
    a.owner_email,
    a.owner_role,
    a.first_order,
    users.client,
    CASE
        WHEN
            SUM(CASE WHEN a.t > add_months(current_date(), -6) AND a.t <= current_date() THEN a.gmv_initial ELSE 0 END)
                OVER (PARTITION BY a.user_id) > 100000 THEN 'big client'
        WHEN
            SUM(CASE WHEN a.t > add_months(current_date(), -6) AND a.t <= current_date() THEN a.gmv_initial ELSE 0 END)
                OVER (PARTITION BY a.user_id) > 30000 THEN 'medium client'
        ELSE 'small client'
    END AS current_client
FROM after_second_qrt_new_order AS a
LEFT JOIN users ON a.user_id = users.user_id AND a.t = users.day
LEFT JOIN country AS c ON a.user_id = c.user_id
WHERE a.gmv_initial > 0 or
    a.order_id in ( '67606db7f7660794452fa237',
                    '6765968e7c2acf46df9a3a82',
                    '6797e073e1842903112967a7',
                    '67992de23e283bd9790a7bff',
                    '679a8be56fe9614353c8244f',
                    '679bb58763be474e81bdc75c',
                    '679bc9a163be474e81bdc883',
                    '67a3aa15950775cbf83f5a77',
                    '67a3ac0d950775cbf83f5a8b',
                    '67a50e37ac4136f0de8615f2',
                    '67a66860ad358c9c8df75052',
                    '67aa5dec51715bab47e68682',
                    '67aa54a951715bab47e68664',
                    '67aa5efd51715bab47e68690',
                    '67aa4b5e47d717642ccf41ca',
                    '67aa4ff1e7e15b582830f223',
                    '67abaaf1e7e15b58283133bf',
                    '67ae340471e3ebc3ad12b011',
                    '67ae365b618b75db93159c28',
                    '67b6344cc42752d15569963d',
                    '67b4ef69a01a855e7f35896f',
                    '67b4cc0da01a855e7f35876d',
                    '67b4ca56a01a855e7f35874c',
                    '67b4f0df1050f68df2141e50',
                    '67b6372e236512cfec8e0583',
                    '67b78e2a2797a9eb03d56778',
                    '67b780892797a9eb03d56758',
                    '67b8b60a12b9c436847b8749',
                    '67b8ba900e3991c09fa501f5',
                    '67bf930d23ab3402f6fdc371',
                    '67c213f4123d7dbb33dc1351',
                    '67c211ac9465c86bf826d266',
                    '67bcd0a04c87410d3c891e6b',
                    '67c04f12b8ef5126941afd35'
    )
