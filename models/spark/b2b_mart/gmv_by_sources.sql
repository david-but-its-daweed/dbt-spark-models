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

SELECT
    CASE
        WHEN order_id = '66fd958cec11d5d880d7ba68' THEN CAST('2024-10-10' AS DATE)
        WHEN order_id = '67085785af548d5f4a765b41' THEN CAST('2024-10-11' AS DATE)
        WHEN order_id = '67002b6fd209d6e661514f43' THEN CAST('2024-10-11' AS DATE)
        WHEN order_id = '6708070daf548d5f4a765ab5' THEN CAST('2024-10-18' AS DATE)
        WHEN order_id = '67100be95b651d83f456c2c2' THEN CAST('2024-10-21' AS DATE)
        WHEN order_id = '6706cf558e0c878cdb3f7ce7' THEN CAST('2024-10-21' AS DATE)
        WHEN order_id = '671ba3300c96a194287a269b' THEN CAST('2024-10-25' AS DATE)
        WHEN order_id = '6717f865b041cd7b62d34daf' THEN CAST('2024-10-28' AS DATE)
        WHEN order_id = '670fd3f45b651d83f456c0dc' THEN CAST('2024-10-28' AS DATE)
        WHEN order_id = '6718ee957df3ac3b21ff9685' THEN CAST('2024-10-29' AS DATE)
        WHEN order_id = '671a7f758f8f5d11638e8526' THEN CAST('2024-10-29' AS DATE)
        WHEN order_id = '671fde6c67fab66c22a898c7' THEN CAST('2024-10-31' AS DATE)
        WHEN order_id = '672bd1eae546571cec885466' THEN CAST('2024-11-08' AS DATE)
        WHEN order_id = '671a91380c96a194287a22b8' THEN CAST('2024-11-04' AS DATE)
        WHEN order_id = '6735ffb2f182e3f4a318c0ee' THEN CAST('2024-11-14' AS DATE)
        WHEN order_id = '672cd8ed1656e9dd7f4e785c' THEN CAST('2024-11-27' AS DATE)
        WHEN order_id = '673b8dd150f7107c6a2ee8f0' THEN CAST('2024-11-29' AS DATE)
        WHEN order_id = '6748ed307fca6ba802a35ebd' THEN CAST('2024-12-06' AS DATE)
        ELSE t
    END AS t,
    order_id,
    CASE
        WHEN order_id = '65a7d6926b3328d3107aaf8b' THEN 9311.0
        WHEN order_id = '65b151c026cb950b2054ff08' THEN 1055.05
        WHEN order_id = '65ddee5f559a3431f0942a29' THEN 28871.0
        WHEN order_id = '65ea0ca5e0670f6be1b0674f' THEN 22552.0
        WHEN order_id = '65f47dc14afeb164f204a60e' THEN 27480.0
        WHEN order_id = '65d51196cc7ce516c12e556c' THEN 2996.04
        WHEN order_id = '65f879194bfeb2b0071f2d8d' THEN 5570.0
        WHEN order_id = '65ef0304bb34fb6e4a4bf64c' THEN 32460.87
        WHEN order_id = '66017fabf2f43fb0b8bd588a' THEN 27577.66
        WHEN order_id = '666ab889ada870860530d538' THEN 1260.25
        WHEN order_id = '666ad15102b33991c4fab1e8' THEN 8462.52
        WHEN order_id = '666aa5aa02b33991c4faac77' THEN 13500.0
        WHEN order_id = '666ab8b95381d9c975574314' THEN 11516.3
        WHEN order_id = '66ba472d0c9bbbddd732b98a' THEN 17917.27
        WHEN order_id = '66c4fe0d0b5bb457ba720a7d' THEN 51978.08
        WHEN order_id = '66cf7db222277dec7ffb5f30' THEN 10517.82
        WHEN order_id = '66cf94f597a2b9194accddb6' THEN 19103.09
        WHEN order_id = '66d71bc6146d7bf49a26fc9f' THEN 8707.74
        WHEN order_id = '66db257b3c2cd05303e83cf3' THEN 13208.86
        WHEN order_id = '66e88b581fa8d70be87e7fa2' THEN 9720.97
        WHEN order_id = '66e3dcbfb561cb7ea36b4617' THEN 10448.85
        WHEN order_id = '66fd958cec11d5d880d7ba68' THEN 11732.0
        WHEN order_id = '67085785af548d5f4a765b41' THEN 20582.0
        WHEN order_id = '67002b6fd209d6e661514f43' THEN 23667.0
        WHEN order_id = '6708070daf548d5f4a765ab5' THEN 11367.05
        WHEN order_id = '67100be95b651d83f456c2c2' THEN 9299.11
        WHEN order_id = '6706cf558e0c878cdb3f7ce7' THEN 14230.48
        WHEN order_id = '671ba3300c96a194287a269b' THEN 19930.39
        WHEN order_id = '6717f865b041cd7b62d34daf' THEN 8507.02
        WHEN order_id = '670fd3f45b651d83f456c0dc' THEN 68300.0
        WHEN order_id = '6718ee957df3ac3b21ff9685' THEN 76777.33
        WHEN order_id = '671a7f758f8f5d11638e8526' THEN 5420.32
        WHEN order_id = '671fde6c67fab66c22a898c7' THEN 9061.22
        WHEN order_id = '672bd1eae546571cec885466' THEN 28837.93
        WHEN order_id = '671a91380c96a194287a22b8' THEN 15665.74
        WHEN order_id = '6735ffb2f182e3f4a318c0ee' THEN 14175.91
        WHEN order_id = '672cd8ed1656e9dd7f4e785c' THEN 15744.0
        WHEN order_id = '673b8dd150f7107c6a2ee8f0' THEN 51098.0
        WHEN order_id = '6748ed307fca6ba802a35ebd' THEN 29212.0
        ELSE gmv_initial
    END AS gmv_initial,
    CASE
        WHEN order_id = '65a7d6926b3328d3107aaf8b' THEN 0.0
        WHEN order_id = '65b151c026cb950b2054ff08' THEN 0.0
        WHEN order_id = '65ddee5f559a3431f0942a29' THEN -698.13
        WHEN order_id = '65ea0ca5e0670f6be1b0674f' THEN -248.0
        WHEN order_id = '65f47dc14afeb164f204a60e' THEN -450.69
        WHEN order_id = '65d51196cc7ce516c12e556c' THEN 0.0
        WHEN order_id = '65f879194bfeb2b0071f2d8d' THEN -168.0
        WHEN order_id = '65ef0304bb34fb6e4a4bf64c' THEN -504.0
        WHEN order_id = '66017fabf2f43fb0b8bd588a' THEN -336.0
        WHEN order_id = '666ab889ada870860530d538' THEN 0.0
        WHEN order_id = '666ad15102b33991c4fab1e8' THEN 241.5
        WHEN order_id = '666aa5aa02b33991c4faac77' THEN -168.0
        WHEN order_id = '666ab8b95381d9c975574314' THEN 97.5
        WHEN order_id = '66ba472d0c9bbbddd732b98a' THEN -2075.31
        WHEN order_id = '66c4fe0d0b5bb457ba720a7d' THEN 221.07
        WHEN order_id = '66cf7db222277dec7ffb5f30' THEN -606.05
        WHEN order_id = '66cf94f597a2b9194accddb6' THEN -650.13
        WHEN order_id = '66d71bc6146d7bf49a26fc9f' THEN 166.74
        WHEN order_id = '66db257b3c2cd05303e83cf3' THEN -2628.41
        WHEN order_id = '66e88b581fa8d70be87e7fa2' THEN 236.43
        WHEN order_id = '66e3dcbfb561cb7ea36b4617' THEN -515.0
        WHEN order_id = '66fd958cec11d5d880d7ba68' THEN -3999.24
        WHEN order_id = '67085785af548d5f4a765b41' THEN 604.0
        WHEN order_id = '67002b6fd209d6e661514f43' THEN 640.0
        WHEN order_id = '6708070daf548d5f4a765ab5' THEN 242.38
        WHEN order_id = '67100be95b651d83f456c2c2' THEN 155.53
        WHEN order_id = '6706cf558e0c878cdb3f7ce7' THEN -3366.88
        WHEN order_id = '671ba3300c96a194287a269b' THEN -2706.34
        WHEN order_id = '6717f865b041cd7b62d34daf' THEN -270.18
        WHEN order_id = '670fd3f45b651d83f456c0dc' THEN -3358.72
        WHEN order_id = '6718ee957df3ac3b21ff9685' THEN -17954.57
        WHEN order_id = '671a7f758f8f5d11638e8526' THEN -1028.77
        WHEN order_id = '671fde6c67fab66c22a898c7' THEN -2225.68
        WHEN order_id = '672bd1eae546571cec885466' THEN -5665.12
        WHEN order_id = '671a91380c96a194287a22b8' THEN -739.83
        WHEN order_id = '6735ffb2f182e3f4a318c0ee' THEN 271.00
        WHEN order_id = '672cd8ed1656e9dd7f4e785c' THEN -208.64
        WHEN order_id = '673b8dd150f7107c6a2ee8f0' THEN -8593.85
        WHEN order_id = '6748ed307fca6ba802a35ebd' THEN -1866.96
        ELSE initial_gross_profit
    END AS initial_gross_profit,

    CASE
        WHEN order_id = '65a7d6926b3328d3107aaf8b' THEN 0.0
        WHEN order_id = '65b151c026cb950b2054ff08' THEN 0.0
        WHEN order_id = '65ddee5f559a3431f0942a29' THEN -698.13
        WHEN order_id = '65ea0ca5e0670f6be1b0674f' THEN -248.0
        WHEN order_id = '65f47dc14afeb164f204a60e' THEN -450.69
        WHEN order_id = '65d51196cc7ce516c12e556c' THEN 0.0
        WHEN order_id = '65f879194bfeb2b0071f2d8d' THEN -168.0
        WHEN order_id = '65ef0304bb34fb6e4a4bf64c' THEN -504.0
        WHEN order_id = '66017fabf2f43fb0b8bd588a' THEN -336.0
        WHEN order_id = '666ab889ada870860530d538' THEN 0.0
        WHEN order_id = '666ad15102b33991c4fab1e8' THEN 241.5
        WHEN order_id = '666aa5aa02b33991c4faac77' THEN -168.0
        WHEN order_id = '666ab8b95381d9c975574314' THEN 97.5
        WHEN order_id = '66ba472d0c9bbbddd732b98a' THEN -2075.31
        WHEN order_id = '66c4fe0d0b5bb457ba720a7d' THEN 221.07
        WHEN order_id = '66cf7db222277dec7ffb5f30' THEN -606.05
        WHEN order_id = '66cf94f597a2b9194accddb6' THEN -650.13
        WHEN order_id = '66d71bc6146d7bf49a26fc9f' THEN 166.74
        WHEN order_id = '66db257b3c2cd05303e83cf3' THEN -2628.41
        WHEN order_id = '66e88b581fa8d70be87e7fa2' THEN 236.43
        WHEN order_id = '66e3dcbfb561cb7ea36b4617' THEN -515.0
        WHEN order_id = '66fd958cec11d5d880d7ba68' THEN -3999.24
        WHEN order_id = '67085785af548d5f4a765b41' THEN 604.0
        WHEN order_id = '67002b6fd209d6e661514f43' THEN 640.0
        WHEN order_id = '6708070daf548d5f4a765ab5' THEN 242.38
        WHEN order_id = '67100be95b651d83f456c2c2' THEN 155.53
        WHEN order_id = '6706cf558e0c878cdb3f7ce7' THEN -3366.88
        WHEN order_id = '671ba3300c96a194287a269b' THEN -2706.34
        WHEN order_id = '6717f865b041cd7b62d34daf' THEN -270.18
        WHEN order_id = '670fd3f45b651d83f456c0dc' THEN -3358.72
        WHEN order_id = '6718ee957df3ac3b21ff9685' THEN -17954.57
        WHEN order_id = '671a7f758f8f5d11638e8526' THEN -1028.77
        WHEN order_id = '671fde6c67fab66c22a898c7' THEN -2225.68
        WHEN order_id = '672bd1eae546571cec885466' THEN -5665.12
        WHEN order_id = '671a91380c96a194287a22b8' THEN -739.83
        WHEN order_id = '6735ffb2f182e3f4a318c0ee' THEN 271.00
        WHEN order_id = '672cd8ed1656e9dd7f4e785c' THEN -208.64
        WHEN order_id = '673b8dd150f7107c6a2ee8f0' THEN -8593.85
        WHEN order_id = '6748ed307fca6ba802a35ebd' THEN -1866.96
        ELSE final_gross_profit
    END AS final_gross_profit,
    utm_campaign,
    utm_source,
    utm_medium,
    source,
    type,
    campaign,
    retention,
    user_id,
    country,
    owner_email,
    owner_role,
    first_order,
    client,
    current_client
FROM {{ ref('gmv_by_sources_wo_filters') }}
WHERE order_id NOT IN (
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
    '671be7a60c96a194287a2766', '6720dff3ea371c645df9b48f', '67212806ea371c645df9b5a5'
)
