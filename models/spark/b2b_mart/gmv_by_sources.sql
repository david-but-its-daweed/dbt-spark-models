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
    t,
    order_id,
    case 
    when order_id = '65a7d6926b3328d3107aaf8b' then 9311.0 
    when order_id = '65b151c026cb950b2054ff08' then 1055.05 
    when order_id = '65ddee5f559a3431f0942a29' then 28871.0 
    when order_id = '65ea0ca5e0670f6be1b0674f' then 22552.0 
    when order_id = '65f47dc14afeb164f204a60e' then 27480.0 
    when order_id = '65d51196cc7ce516c12e556c' then 2996.04 
    when order_id = '65f879194bfeb2b0071f2d8d' then 5570.0 
    when order_id = '65ef0304bb34fb6e4a4bf64c' then 32460.87 
    when order_id = '66017fabf2f43fb0b8bd588a' then 27577.66 
    when order_id = '666ab889ada870860530d538' then 1260.25 
    when order_id = '666ad15102b33991c4fab1e8' then 8462.52 
    when order_id = '666aa5aa02b33991c4faac77' then 13500.0 
    when order_id = '666ab8b95381d9c975574314' then 11516.3 
    when order_id = '66ba472d0c9bbbddd732b98a' then 17917.27 
    when order_id = '66c4fe0d0b5bb457ba720a7d' then 51978.08 
    when order_id = '66cf7db222277dec7ffb5f30' then 10517.82 
    when order_id = '66cf94f597a2b9194accddb6' then 19103.09 
    when order_id = '66d71bc6146d7bf49a26fc9f' then 8707.74 
    when order_id = '66db257b3c2cd05303e83cf3' then 13208.86 
    when order_id = '66e88b581fa8d70be87e7fa2' then 9720.97 
    when order_id = '66e3dcbfb561cb7ea36b4617' then 10448.85 
    when order_id = '66fd958cec11d5d880d7ba68' then 11732.0 
    when order_id = '67085785af548d5f4a765b41' then 20582.0 
    when order_id = '67002b6fd209d6e661514f43' then 23667.0 
    when order_id = '6708070daf548d5f4a765ab5' then 11367.05 
    when order_id = '67100be95b651d83f456c2c2' then 9299.11 
    when order_id = '6706cf558e0c878cdb3f7ce7' then 14230.48 
    when order_id = '671ba3300c96a194287a269b' then 19930.39 
    when order_id = '6717f865b041cd7b62d34daf' then 8507.02 
    when order_id = '670fd3f45b651d83f456c0dc' then 68300.0 
    when order_id = '6718ee957df3ac3b21ff9685' then 76777.33 
    when order_id = '671a7f758f8f5d11638e8526' then 5420.32 
    when order_id = '671fde6c67fab66c22a898c7' then 9061.22 
    when order_id = '672bd1eae546571cec885466' then 28837.93 
    when order_id = '671a91380c96a194287a22b8' then 15665.74
    else gmv_initial end gmv_initial,
    case 
    when order_id = '65a7d6926b3328d3107aaf8b' then 0.0 
    when order_id = '65b151c026cb950b2054ff08' then 0.0 
    when order_id = '65ddee5f559a3431f0942a29' then -698.13 
    when order_id = '65ea0ca5e0670f6be1b0674f' then -248.0 
    when order_id = '65f47dc14afeb164f204a60e' then -450.69 
    when order_id = '65d51196cc7ce516c12e556c' then 0.0 
    when order_id = '65f879194bfeb2b0071f2d8d' then -168.0 
    when order_id = '65ef0304bb34fb6e4a4bf64c' then -504.0 
    when order_id = '66017fabf2f43fb0b8bd588a' then -336.0 
    when order_id = '666ab889ada870860530d538' then 0.0 
    when order_id = '666ad15102b33991c4fab1e8' then 241.5 
    when order_id = '666aa5aa02b33991c4faac77' then -168.0 
    when order_id = '666ab8b95381d9c975574314' then 97.5 
    when order_id = '66ba472d0c9bbbddd732b98a' then -2075.31 
    when order_id = '66c4fe0d0b5bb457ba720a7d' then 221.07 
    when order_id = '66cf7db222277dec7ffb5f30' then -606.05 
    when order_id = '66cf94f597a2b9194accddb6' then -650.13 
    when order_id = '66d71bc6146d7bf49a26fc9f' then 166.74 
    when order_id = '66db257b3c2cd05303e83cf3' then -2628.41 
    when order_id = '66e88b581fa8d70be87e7fa2' then 236.43 
    when order_id = '66e3dcbfb561cb7ea36b4617' then -515.0 
    when order_id = '66fd958cec11d5d880d7ba68' then -3999.24 
    when order_id = '67085785af548d5f4a765b41' then 604.0 
    when order_id = '67002b6fd209d6e661514f43' then 640.0 
    when order_id = '6708070daf548d5f4a765ab5' then 242.38 
    when order_id = '67100be95b651d83f456c2c2' then 155.53 
    when order_id = '6706cf558e0c878cdb3f7ce7' then -3366.88 
    when order_id = '671ba3300c96a194287a269b' then -2706.34 
    when order_id = '6717f865b041cd7b62d34daf' then -270.18 
    when order_id = '670fd3f45b651d83f456c0dc' then -3358.72 
    when order_id = '6718ee957df3ac3b21ff9685' then -17954.57 
    when order_id = '671a7f758f8f5d11638e8526' then -1028.77 
    when order_id = '671fde6c67fab66c22a898c7' then -2225.68 
    when order_id = '672bd1eae546571cec885466' then -5665.12 
    when order_id = '671a91380c96a194287a22b8' then -739.83
    else initial_gross_profit as initial_gross_profit,

    case
    when order_id = '65a7d6926b3328d3107aaf8b' then 0.0 
    when order_id = '65b151c026cb950b2054ff08' then 0.0 
    when order_id = '65ddee5f559a3431f0942a29' then -698.13 
    when order_id = '65ea0ca5e0670f6be1b0674f' then -248.0 
    when order_id = '65f47dc14afeb164f204a60e' then -450.69 
    when order_id = '65d51196cc7ce516c12e556c' then 0.0 
    when order_id = '65f879194bfeb2b0071f2d8d' then -168.0 
    when order_id = '65ef0304bb34fb6e4a4bf64c' then -504.0 
    when order_id = '66017fabf2f43fb0b8bd588a' then -336.0 
    when order_id = '666ab889ada870860530d538' then 0.0 
    when order_id = '666ad15102b33991c4fab1e8' then 241.5 
    when order_id = '666aa5aa02b33991c4faac77' then -168.0 
    when order_id = '666ab8b95381d9c975574314' then 97.5 
    when order_id = '66ba472d0c9bbbddd732b98a' then -2075.31 
    when order_id = '66c4fe0d0b5bb457ba720a7d' then 221.07 
    when order_id = '66cf7db222277dec7ffb5f30' then -606.05 
    when order_id = '66cf94f597a2b9194accddb6' then -650.13 
    when order_id = '66d71bc6146d7bf49a26fc9f' then 166.74 
    when order_id = '66db257b3c2cd05303e83cf3' then -2628.41 
    when order_id = '66e88b581fa8d70be87e7fa2' then 236.43 
    when order_id = '66e3dcbfb561cb7ea36b4617' then -515.0 
    when order_id = '66fd958cec11d5d880d7ba68' then -3999.24 
    when order_id = '67085785af548d5f4a765b41' then 604.0 
    when order_id = '67002b6fd209d6e661514f43' then 640.0 
    when order_id = '6708070daf548d5f4a765ab5' then 242.38 
    when order_id = '67100be95b651d83f456c2c2' then 155.53 
    when order_id = '6706cf558e0c878cdb3f7ce7' then -3366.88 
    when order_id = '671ba3300c96a194287a269b' then -2706.34 
    when order_id = '6717f865b041cd7b62d34daf' then -270.18 
    when order_id = '670fd3f45b651d83f456c0dc' then -3358.72 
    when order_id = '6718ee957df3ac3b21ff9685' then -17954.57 
    when order_id = '671a7f758f8f5d11638e8526' then -1028.77 
    when order_id = '671fde6c67fab66c22a898c7' then -2225.68 
    when order_id = '672bd1eae546571cec885466' then -5665.12 
    when order_id = '671a91380c96a194287a22b8' then -739.83
    else final_gross_profit end final_gross_profit,
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
WHERE order_id NOT IN ('657c58febbbdb8729dd7d39e', '658d3fc317e10341173c1f20', '659d3c4dddc19670cf999989', 
    '65aa2a7ff11499f63900def9',
    '643d72b2acb1823e59eab7c5',
    '65e9c5b30d65cd6752992149',
    '663b87b734a8066e53968a66',
    '665f0ae08a8dac1a3ee66e1d',
    '666374d84c64a4aeb7c9aecb',
    '669ac6f8c980840fd9e5f2b7',
    '66a7aab79aa8c1361243e9d2','66b388e4c5ad160d2d8de692','66b3b9bc877fd3da349bb8ef',
    '66b1223b87628c73dce32443','66c4f62e0b5bb457ba720a4f','66ccad998d98f755caa68ad9',
    '66d20c48329656118385ec5a','66d5f6bcace18af6ab5d4860','66ce11498d98f755caa6923b',
    '66d9c9a1b1fcc25b92ce3f1d','66da0c24182675e7411f2a28','66e2f6c283d6709f08a84b03',
    '66ed74676f09959ab0fd0f12','66ec20d252035764e906295a','66f300b4e1ed52479981bd57',
    '66f5a143210729de9b82999e','66fd455b0052353d1ce86fc3','66fd5b223beac4b744f2c699',
    '670fa15a5b651d83f456bd54','67090fcf4348d72dc52e7e89','67100ba05b651d83f456c2bb',
    '671be7a60c96a194287a2766','6720dff3ea371c645df9b48f','67212806ea371c645df9b5a5'
    )
