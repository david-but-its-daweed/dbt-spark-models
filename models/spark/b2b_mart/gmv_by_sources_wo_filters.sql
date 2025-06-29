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
    WHERE
        (order_id IN (SELECT DISTINCT order_id FROM {{ ref('joom_pro_manual_orders') }}) AND fo.next_effective_ts_msk IS NULL)
        OR
        (
            fo.last_order_status < 60
            AND fo.last_order_status >= 10
            AND fo.next_effective_ts_msk IS NULL
            AND fo.min_manufactured_ts_msk IS NOT NULL
        )
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
WHERE  (a.gmv_initial > 0 or
    a.order_id in ( '67606db7f7660794452fa237',
                    '6765968e7c2acf46df9a3a82',
                    '6797e073e1842903112967a7',
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
                    '67c04f12b8ef5126941afd35',
                    '67dc1e96f003ccfe31774747',
                    '672bd1eae546571cec885466',
                    '67d8e863e3d32bddca94311a',
    '6842923e84960546832e6529',
'682494ae9a0f0f6479a3bd80',
'682b885f88bcd532c538b6a3',
'682c322edc982227b95dffd9',
'682b85cc204b6aadb4660ce9',
'68341c5e3b29eebca4514bea',
'684045673322b884d25229e9',
'6840472ba4d45b8a03a2c0f7',
'68396a10fc4d57741a90637d',
'68396a6a9b319d9974d6ffbb',
'68341daf25f493cec27996b0',
'68394d4afc4d57741a7c448f',
'6830caac14601207bc6f52c4',
'683415b1ebb6de221bcbec68',
'68341a15126683c73fd74124',
'68341547e7363fefa6980ff6',
'6836cc40f2c8c8e4e8138d99',
'6836c60ebbad2b945a816785',
'683958c3e89c54963c317943',
'6834ba74deab0f24ab079276',
'6835d08f0af71c93a2f12aa6',
'6836dc49f2c8c8e4e818a7d6',
'6836c9fcb45ffcda536e824e',
'6836c8ebbbad2b945a83b225',
'683952c235700613097e2d9e',
'6835bd48344739337b7b8078',
'682cbeb76126ef7a9d0e5f59',
'6835fd80d3e8752de5592e05',
'68395028fd19012569a4a82a',
'683951befd19012569a5ea67',
'68384a60913c0ecccfed04df',
'68395137c28ac06fe4efc95b',
'6835fbe350f8d973e2f0a50d',
'6839676274d9d4ab97ad949b',
'6836dbe4afb17beef24e2d30',
'6838d18c17805cbf42b99c58',
'683957963570061309821f23',
'683955d474d9d4ab97a1fbd9',
'683760640f1a3362adfcae27',
'68394dbe357006130979e112',
'68395661fc4d57741a84b323',
'6840030bad09c7951d123910',
'6839659f78ba761adeed3795',
'683962dc19bdaaac832ef367',
'68395c90fc4d57741a8a76ff',
'683967e278ba761adeedc911',
'683960ec78ba761adeebfd9b',
'68396664fd19012569b25037',
'683962819b319d9974d442ad',
'68395edafc4d57741a8c2c17',
'683993da651dfa84c81311d0',
'684019ed3af280e7e1c43736',
'683a0ea8bd9661239a655013',
'683a049dbd9661239a654fe1',
'683d59cd3570061309dac293',
'684004e5ad09c7951d12e178',
'68402937ad09c7951d1c4d53',
'68402daa3af280e7e1c865e4',
'68400148382e75f456c95554',
'68401bc2ad09c7951d1988ab',
'6840037be44843f6fef2d21d',
'684005ac04082f409f024b4c',
'68401e01690f00a8e4df55ac',
'68401b50ad09c7951d196797',
'68402cbf3322b884d24b02e4',
'68402ba1d44faa59b76e53d5',
'68402a3eb14a57cf24073948',
'6840466f04082f409f1318f4',
'683f028d7933a6db28824289',
'684018d93322b884d2455e03',
'6840305e3322b884d24bd0c4',
'68402fc267c88980f9af75e2',
'684030d8382e75f456d6e898',
'68414a7d690f00a8e43def02',
'683f4d687933a6db288243e9',
'6840952fa4d45b8a03a2c291',
'684094b27933a6db28824af3',
'683f56bf7933a6db28824408',
'683a0ea2651dfa84c81313d0',
'6841ed0d127c422b7471a161',
'6841f5d3127c422b7471a18f',
'683df70ff402f389e80e15e5',
'6843396a7330e6e835504439',
'68404d497933a6db2882499e',
'6841f7d9127c422b7471a1a6',
'6841fa54127c422b7471a1b7',
'6841f232b4d67eca1cd65b3b',
'684338357330e6e835504410'
                
    ))
and a.order_id not in (
    '68487cc706f123c6aee11638',
    '6847480b5463b5bd0a992161',
    '67f045501dc7dc786ce7f226',
'6806d2a9d7e3d70238f84911',
'67f9e26d17ff257b1adf332f',
'68024258bff0d9d96c4c8568',
'6807d410dd735a7b827dcb63',
'682b5ccb9844bba634a873ff',
'682175bc4ffdc7744fea991a',
'68127549b83eff29cc654b18',
'68113fde15d2d27142c0d51c',
'682f4e650d521cf7e7de8e75',
'683068455b5347cf96789fda',
'68217784bd6c02683a569453',
'6825fa50c193cd372175aebc',
'682b885a8872915e7647e9e1',
'6824916bcf72679d39bc348c',
'682b88d23eec7f326bb605b0',
'6825fc746b4b1b3fee79c924',
'682c35e5188bacf0fd4e0b25',
'682495819a0f0f6479a3ea95',
'6824b1d8ac19af38c3c291c3',
'682c38f1fc70b3027e9cc9f8',
'6824b232819754eb34adced2',
'6825ce018ee304ab9d12f87b',
'68305b49526e8f9664044ab9',
'68305bda526e8f9664047dd1',
'683034d69e465f6efcfdab43',
'68306bf0526e8f966408bdfc',
'683068a8132d7bf01819531e',
'68304be3526e8f966400452c',
'682f24f63eb2a63d5b548067',
'682f5a895cf8519b0f824134',
'682f58df5cf8519b0f82411a',
            '6776e4f7dfbe6dd933205c82',
'677e7b72dfbe6dd93320672e',
'677ed6727104bc79f2a8bc12',
'678692cc2583002a9a42f608',
'678567232583002a9a42efa2',
'678805352583002a9a42fb85',
'6788e90a5362eed40410e435',
'678ea4029ff44121e2a35049',
'6792d97fc36f2c7f811dc342',
'6797885eb00d9e8df2652347',
'67975eb7b00d9e8df265210a',
'6797f63dcc18a9df8ad577c1',
'6797dbbce184290311296798',
'6797ab55cc18a9df8ad57707',
'679a3b075760b420efa32f75',
'679929d385fa3855c0b5740f',
'67992de23e283bd9790a7bff',
'679a2ac711a5940bca86ab0c',
'679cdc7661c09318cde7c840',
'679d2b1861c09318cde8191f',
'679d3b6561c09318cde819ea',
'67a10ea072b672afc4f47b85',
'67a38493e5e4959a3b39f0f1',
'67a38149e5e4959a3b39f0cb',
'67aa44af47d717642ccf3d76',
'67aa3fd147d717642ccf3d61',
'67a661d1a0df8d70aee629b3',
'67aa2734a0df8d70aee62e6e',
'67ab57ace7e15b58283132b8',
'67af8585ab7537262edf2e08',
'67af9893ab7537262edf2e79',
'67b48e96740b1af0a1983311',
'67b49d5fd9b38f7cfe2bc2fa',
'67b4916cd9b38f7cfe2bc2c3',
'67b4f6821050f68df2141ea8',
'67b4df5b1050f68df2141d0b',
'67b4f7001050f68df2141ebf',
'67b636a2c42752d15569966d',
'67b8b8ee0e3991c09fa501e2',
'67b7e7d2c8aeeff56e1e0693',
'67b7d873c8aeeff56e1e0674',
'67b8de2f0e3991c09fa50268',
'67bdaf596625fbade5dedd00',
'67bc9a866625fbade5ded8dd',
'67bd07ec4c87410d3c891f0f',
'67be4ef2d4c7ca98e6aff1d8',
'67af4f32ab7537262edf2d66',
'67ab5923e7e15b58283132db',
'67ab5f4551715bab47e6a843',
'67adf2b871e3ebc3ad12ae49',
'679156a20d845cb3bb23d5fa',
'6793ca0b537d8fbd6e3bd8ce',
'679b809850edc4f7f8a21f59',
'67b63d64236512cfec8e05a9',
'67b5ce7d38476941472b96ee',
'67b78e2a2797a9eb03d56778',
'67c0cdb94c655ff8a056e1eb',
'67c9cbed5e266a1c79c68c26',
'679411acd253e1d017ad99b7',
'679bb58763be474e81bdc75c',
'679a8be56fe9614353c8244f',
'67ae365b618b75db93159c28',
'67cb1f1e48facf1263a9210b',
'67bdc6cbc754ab13a73a54b4',
'67d2162d1ea8f08a0e820dcb',
'67cf3f3eecad568f7a1ed8a1',
'67c04f12b8ef5126941afd35',
'67c8b77c40583cbaa1f809c9',
'67cf401116c439133e297fac',
'67cf41cdecad568f7a1ed8cc',
'67d0a04ec2ea1faefc29e928',
'67d06a3423d6d02a00002eff',
'67d20f5873dfd12eb9ef1284',
'67d1d1d173dfd12eb9ef0f88',
'67d3364cb98e2bcdcee82126',
'67b8b60a12b9c436847b8749',
'67d89c55e3d32bddca943096',
'67c213f4123d7dbb33dc1351',
'67c211ac9465c86bf826d266',
'67da0b40e0689a2e6e961a92',
'67d41795fcdc826751774927',
'67d4486efcdc826751774aef',
'67deee463970bba68b089cfd',
'67db5f8fe0689a2e6e9620b1',
'67dc6e583b6185b8cd53e4ac',
'67dc71f9f8e513f1706dc216',
'67dc76adf8e513f1706dc23f',
'67dc7c4f3b6185b8cd53e511',
'67ddc00535cd58dea8f8f265',
'67aa4ff1e7e15b582830f223',
'67c818161660a7dee86e9139',
'67c1f4c2123d7dbb33dc12dd',
'67e5e90740b241cc8f0f4714',
'67de009f35cd58dea8f8f2d2',
'67d9fffce0689a2e6e961a25',
'67d95497e3d32bddca943509',
'67de210335cd58dea8f8f306',
'67de19583970bba68b089c8f',
'67dc7473f8e513f1706dc22f',
'67ddc8133970bba68b089bd7',
'67de03fb35cd58dea8f8f2e0',
'67e1c1362f6685e457154051',
'67dda2c42e11c8c002cdadc6',
'67e15c94c9907af4761bb1c0',
'67dd98ac3970bba68b089b07',
'67e1c02d7088473fd78a48d5',
'67e19f39755dbc416c788b4a',
'67e19cc6755dbc416c788b35',
'67e1e1067088473fd78a4917',
'67e4343828d1a6a4fe344a1b',
'67e33b8d8ba0d53279ab5a5d',
'67e3f2c67a02c3ca65c796cc',
'67e484792b3df5470d8dd0ca',
'67e483a02b3df5470d8dd0b9',
'67e484e22b3df5470d8dd0d8',
'67eac09d5a014b6e33720e7d',
'67e5906040b241cc8f0f467a',
'67e69a2140b241cc8f0f4af3',
'67e704a5090cb22f1c23b13e',
'67ea14ff090cb22f1c23b359',
'67e7291750c57b09968b4ebb',
'67ea90eac8065bd219534829',
'67ec2aa8b55352d20f5960fb',
'67ec342b7f496482d1c8c4fc',
'67ed9e56786f5bca82dd0909',
'67efad4a6d54924bca937606',
'67eeeb425687cab15b2c0354',
'67f41bbde052e333eb76836d',
'67f512a0f1d64fc18ad6640e',
'67f6da6a40cb1bbde55ce3ad',
'67f4333923b8adf3e6545dc1',
'67f6dad140cb1bbde55ce3bb',
'67f6dcc0136841bb988b4c69',
'67f7292cd84e430001ef8d27',
'67f6df313fbbd74f1ad61f03',
'67b76dde2797a9eb03d5670b',
'67ef04b409cb566dcc135376',
'67f5fa3436fc1e135b9c3e06',
'67f6de25136841bb988b4c80',
'67fff7c640ae85e1db4f8844',
'680667ac35189b19986643c0',
'6806d2e40490951e6ada26f0',
'6806671a2be2f70335bc4754',
'680175d395195af36dcaade0',
'680b97be15cae29eeab8d363',
'680b9b40c16758037ab113e8',
'68022d08368397a2391d408c',
'681de1c67bdffb86de51079d'
    )
