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
        WHEN order_id = '6735ffb2f182e3f4a318c0ee' THEN CAST('2024-11-14' AS DATE)
        WHEN order_id = '672cd8ed1656e9dd7f4e785c' THEN CAST('2024-11-27' AS DATE)
        WHEN order_id = '673b8dd150f7107c6a2ee8f0' THEN CAST('2024-11-29' AS DATE)
        WHEN order_id = '6748ed307fca6ba802a35ebd' THEN CAST('2024-12-06' AS DATE)
        WHEN order_id = '675c67042e1d04dfe942898d' THEN CAST('2024-12-12' AS DATE)
        WHEN order_id = '675af7b808433ed244d897e9' THEN CAST('2024-12-12' AS DATE)
        WHEN order_id = '6759f0a917c167b3cf1d5bad' THEN CAST('2024-12-11' AS DATE)
        WHEN order_id = '675b02d4781d3609c1e97225' THEN CAST('2024-12-12' AS DATE)
        WHEN order_id = '675c6ea3efa9f41236d87427' THEN CAST('2024-12-13' AS DATE)
        WHEN order_id = '675c7411efa9f41236d87439' THEN CAST('2024-12-13' AS DATE)
        WHEN order_id = '675c3d6b3703512d44aaa8e0' THEN CAST('2024-12-13' AS DATE)
        WHEN order_id = '675c462f3703512d44aaa91b' THEN CAST('2024-12-13' AS DATE)
        WHEN order_id = '675c69e52e1d04dfe94289c5' THEN CAST('2024-12-13' AS DATE)
        WHEN order_id = '67606db7f7660794452fa237' THEN CAST('2024-12-16' AS DATE)
        WHEN order_id = '67617c09f7660794452fa7f0' THEN CAST('2024-12-16' AS DATE)
        WHEN order_id = '6760651206c420992f837371' THEN CAST('2024-12-16' AS DATE)
        WHEN order_id = '67602b4a2e7d9061a2ee3b4b' THEN CAST('2024-12-16' AS DATE)
        WHEN order_id = '6762d82cf7660794452faf52' THEN CAST('2024-12-18' AS DATE)
        WHEN order_id = '676180dcf7660794452fa831' THEN CAST('2024-12-17' AS DATE)
        WHEN order_id = '6765968e7c2acf46df9a3a82' THEN CAST('2024-12-20' AS DATE)
        WHEN order_id = '675c462f3703512d44aaa91b' THEN CAST('2024-12-13' AS DATE)
       WHEN order_id = '6776e4f7dfbe6dd933205c82' THEN CAST('2025-01-02' AS DATE) 
         WHEN order_id = '677270e67104bc79f2a8ac97' THEN CAST('2025-01-07' AS DATE) 
         WHEN order_id = '677e7b72dfbe6dd93320672e' THEN CAST('2025-01-07' AS DATE) 
         WHEN order_id = '677eafa37104bc79f2a8bb6c' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677eb6a4dfbe6dd933206831' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677edff8dfbe6dd9332068d6' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677ed8b3dfbe6dd9332068b6' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677ed4487104bc79f2a8bc04' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677ed6727104bc79f2a8bc12' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677ee0d67104bc79f2a8bc29' THEN CAST('2025-01-08' AS DATE) 
         WHEN order_id = '677fd23a7104bc79f2a8c055' THEN CAST('2025-01-09' AS DATE) 
         WHEN order_id = '677fe6597104bc79f2a8c0e8' THEN CAST('2025-01-09' AS DATE) 
         WHEN order_id = '6761acb706c420992f837ae0' THEN CAST('2025-01-10' AS DATE)
     WHEN order_id = '671a91380c96a194287a22b8' THEN CAST('2024-10-31' AS DATE) 
     WHEN order_id = '675b40fb3703512d44aaa412' THEN CAST('2024-12-24' AS DATE) 
     WHEN order_id = '6769b2e66cc818d350e04793' THEN CAST('2024-12-27' AS DATE) 
     WHEN order_id = '676ab681270b1f328d8f7cc2' THEN CAST('2024-12-27' AS DATE)
    WHEN order_id = '6784f1bd7294db47e074844d' THEN CAST('2025-01-10' AS DATE) 
 WHEN order_id = '6781628b54880f4bf013c566' THEN CAST('2025-01-10' AS DATE) 
 WHEN order_id = '6781672b4f74fe5dab4f0cbc' THEN CAST('2025-01-10' AS DATE) 
 WHEN order_id = '67815ca154880f4bf013c533' THEN CAST('2025-01-10' AS DATE) 
 WHEN order_id = '6784eb1af7e293bf7267b519' THEN CAST('11.01.2025' AS DATE) 
 WHEN order_id = '678567232583002a9a42efa2' THEN CAST('2025-01-13' AS DATE) 
 WHEN order_id = '6785603c5362eed40410d47c' THEN CAST('2025-01-13' AS DATE) 
 WHEN order_id = '67855e342583002a9a42ef84' THEN CAST('2025-01-13' AS DATE) 
 WHEN order_id = '67868f632583002a9a42f5f6' THEN CAST('2025-01-13' AS DATE) 
 WHEN order_id = '67865e4c5362eed40410d955' THEN CAST('2025-01-13' AS DATE) 
 WHEN order_id = '678692cc2583002a9a42f608' THEN CAST('2025-01-13' AS DATE) 
 WHEN order_id = '67866bd05362eed40410d9b9' THEN CAST('2025-01-14' AS DATE) 
 WHEN order_id = '6788203e2583002a9a42fc1e' THEN CAST('2025-01-15' AS DATE) 
 WHEN order_id = '6787d94b5362eed40410df41' THEN CAST('2025-01-15' AS DATE) 
 WHEN order_id = '678805352583002a9a42fb85' THEN CAST('2025-01-15' AS DATE) 
 WHEN order_id = '6788e90a5362eed40410e435' THEN CAST('2025-01-15' AS DATE) 
 WHEN order_id = '6789660f46dd05f18e97ab1b' THEN CAST('2025-01-16' AS DATE) 
 WHEN order_id = '67897626e4dd3a64d0d31119' THEN CAST('2025-01-16' AS DATE) 
 WHEN order_id = '678970a2e4dd3a64d0d310ff' THEN CAST('2025-01-16' AS DATE) 
 WHEN order_id = '678abd0e7958f9ced43ba45e' THEN CAST('2025-01-16' AS DATE) 
 WHEN order_id = '678ac3e67958f9ced43ba483' THEN CAST('2025-01-17' AS DATE) 
 WHEN order_id = '678aaf15e4dd3a64d0d3193f' THEN CAST('2025-01-17' AS DATE) 
 WHEN order_id = '6740d51a4bd146113e694424' THEN CAST('19.01.2025' AS DATE) 
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
        WHEN order_id = '6759f0a917c167b3cf1d5bad' THEN 1010.1
        WHEN order_id = '675b02d4781d3609c1e97225' THEN 278.95
        WHEN order_id = '675c67042e1d04dfe942898d' THEN 243.30
        WHEN order_id = '675af7b808433ed244d897e9' THEN 685.55
        WHEN order_id = '675c6ea3efa9f41236d87427' THEN 366.5
        WHEN order_id = '675c7411efa9f41236d87439' THEN 365.99
        WHEN order_id = '675c3d6b3703512d44aaa8e0' THEN 2579.54
        WHEN order_id = '675c462f3703512d44aaa91b' THEN 341.22
        WHEN order_id = '675c69e52e1d04dfe94289c5' THEN 16715.72
        WHEN order_id = '67606db7f7660794452fa237' THEN 344.33
        WHEN order_id = '67617c09f7660794452fa7f0' THEN 577.95
        WHEN order_id = '6760651206c420992f837371' THEN 1042.42
        WHEN order_id = '67602b4a2e7d9061a2ee3b4b' THEN 455.66
        WHEN order_id = '6762d82cf7660794452faf52' THEN 201.74
        WHEN order_id = '676180dcf7660794452fa831' THEN 3506.11
        WHEN order_id = '6765968e7c2acf46df9a3a82' THEN 344.67
        WHEN order_id = '675c462f3703512d44aaa91b' THEN 341.22
        WHEN order_id = '6776e4f7dfbe6dd933205c82' THEN 111.34  
         WHEN order_id = '677270e67104bc79f2a8ac97' THEN 12112.92  
         WHEN order_id = '677e7b72dfbe6dd93320672e' THEN 338.54  
         WHEN order_id = '677eafa37104bc79f2a8bb6c' THEN 570.67  
         WHEN order_id = '677eb6a4dfbe6dd933206831' THEN 6359.99  
         WHEN order_id = '677edff8dfbe6dd9332068d6' THEN 356.24  
         WHEN order_id = '677ed8b3dfbe6dd9332068b6' THEN 284.93  
         WHEN order_id = '677ed4487104bc79f2a8bc04' THEN 332.37  
         WHEN order_id = '677ed6727104bc79f2a8bc12' THEN 291.73  
         WHEN order_id = '677ee0d67104bc79f2a8bc29' THEN 509.17  
         WHEN order_id = '677fd23a7104bc79f2a8c055' THEN 392.54  
         WHEN order_id = '677fe6597104bc79f2a8c0e8' THEN 251.53  
         WHEN order_id = '6761acb706c420992f837ae0' THEN 65146.46 
         WHEN order_id = '67002b6fd209d6e661514f43' THEN 24295.62  
     WHEN order_id = '672cd8ed1656e9dd7f4e785c' THEN 15744.37  
     WHEN order_id = '673b8dd150f7107c6a2ee8f0' THEN 51098.27  
     WHEN order_id = '6748ed307fca6ba802a35ebd' THEN 29211.58  
     WHEN order_id = '675b40fb3703512d44aaa412' THEN 12434.53  
     WHEN order_id = '6769b2e66cc818d350e04793' THEN 23511.33  
     WHEN order_id = '676ab681270b1f328d8f7cc2' THEN 35136.69
    WHEN order_id = '6784f1bd7294db47e074844d' THEN 1810.98  
 WHEN order_id = '6781628b54880f4bf013c566' THEN 204.86  
 WHEN order_id = '6781672b4f74fe5dab4f0cbc' THEN 428.22  
 WHEN order_id = '67815ca154880f4bf013c533' THEN 155.25  
 WHEN order_id = '6784eb1af7e293bf7267b519' THEN 675.92  
 WHEN order_id = '678567232583002a9a42efa2' THEN 575.94  
 WHEN order_id = '6785603c5362eed40410d47c' THEN 2911.60  
 WHEN order_id = '67855e342583002a9a42ef84' THEN 426.75  
 WHEN order_id = '67868f632583002a9a42f5f6' THEN 976.27  
 WHEN order_id = '67865e4c5362eed40410d955' THEN 341.02  
 WHEN order_id = '678692cc2583002a9a42f608' THEN 261.89  
 WHEN order_id = '67866bd05362eed40410d9b9' THEN 568.92  
 WHEN order_id = '6788203e2583002a9a42fc1e' THEN 797.11  
 WHEN order_id = '6787d94b5362eed40410df41' THEN 1099.57  
 WHEN order_id = '678805352583002a9a42fb85' THEN 1377.71  
 WHEN order_id = '6788e90a5362eed40410e435' THEN 3443.76  
 WHEN order_id = '6789660f46dd05f18e97ab1b' THEN 120.92  
 WHEN order_id = '67897626e4dd3a64d0d31119' THEN 876.32  
 WHEN order_id = '678970a2e4dd3a64d0d310ff' THEN 393.62  
 WHEN order_id = '678abd0e7958f9ced43ba45e' THEN 4191.91  
 WHEN order_id = '678ac3e67958f9ced43ba483' THEN 250.45  
 WHEN order_id = '678aaf15e4dd3a64d0d3193f' THEN 1629.78  
 WHEN order_id = '6740d51a4bd146113e694424' THEN 5026.04
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
        WHEN order_id = '675c69e52e1d04dfe94289c5' THEN -862.52
        WHEN order_id = '6776e4f7dfbe6dd933205c82' THEN 0.0  
         WHEN order_id = '677270e67104bc79f2a8ac97' THEN 106.79
         WHEN order_id = '677e7b72dfbe6dd93320672e' THEN 0.0  
         WHEN order_id = '677eafa37104bc79f2a8bb6c' THEN 0.0  
         WHEN order_id = '677eb6a4dfbe6dd933206831' THEN 0.0  
         WHEN order_id = '677edff8dfbe6dd9332068d6' THEN 0.0  
         WHEN order_id = '677ed8b3dfbe6dd9332068b6' THEN 0.0  
         WHEN order_id = '677ed4487104bc79f2a8bc04' THEN 0.0  
         WHEN order_id = '677ed6727104bc79f2a8bc12' THEN 0.0  
         WHEN order_id = '677ee0d67104bc79f2a8bc29' THEN 0.0  
         WHEN order_id = '677fd23a7104bc79f2a8c055' THEN 0.0  
         WHEN order_id = '677fe6597104bc79f2a8c0e8' THEN 0.0  
         WHEN order_id = '6761acb706c420992f837ae0' THEN -6677.27 
         WHEN order_id = '6759f0a917c167b3cf1d5bad' THEN 25.25  
         WHEN order_id = '675b02d4781d3609c1e97225' THEN 6.97  
         WHEN order_id = '675c67042e1d04dfe942898d' THEN 6.08  
         WHEN order_id = '675af7b808433ed244d897e9' THEN 17.14  
         WHEN order_id = '675c6ea3efa9f41236d87427' THEN 9.16  
         WHEN order_id = '675c7411efa9f41236d87439' THEN 9.15  
         WHEN order_id = '675c3d6b3703512d44aaa8e0' THEN 64.49  
         WHEN order_id = '675c462f3703512d44aaa91b' THEN 8.53  
         WHEN order_id = '67606db7f7660794452fa237' THEN 8.61  
         WHEN order_id = '67617c09f7660794452fa7f0' THEN 14.45  
         WHEN order_id = '6760651206c420992f837371' THEN 26.06  
         WHEN order_id = '67602b4a2e7d9061a2ee3b4b' THEN 11.39  
         WHEN order_id = '6762d82cf7660794452faf52' THEN 5.04  
         WHEN order_id = '676180dcf7660794452fa831' THEN 87.65  
         WHEN order_id = '6765968e7c2acf46df9a3a82' THEN 8.62  
         WHEN order_id = '675b40fb3703512d44aaa412' THEN -859.6  
         WHEN order_id = '6769b2e66cc818d350e04793' THEN 1444.6  
         WHEN order_id = '676ab681270b1f328d8f7cc2' THEN -3057.86
    WHEN order_id = '6784f1bd7294db47e074844d' THEN 0.00  
 WHEN order_id = '6781628b54880f4bf013c566' THEN 0.00  
 WHEN order_id = '6781672b4f74fe5dab4f0cbc' THEN 0.00  
 WHEN order_id = '67815ca154880f4bf013c533' THEN 0.00  
 WHEN order_id = '6784eb1af7e293bf7267b519' THEN 0.00  
 WHEN order_id = '678567232583002a9a42efa2' THEN 0.00  
 WHEN order_id = '6785603c5362eed40410d47c' THEN 0.00  
 WHEN order_id = '67855e342583002a9a42ef84' THEN 0.00  
 WHEN order_id = '67868f632583002a9a42f5f6' THEN 0.00  
 WHEN order_id = '67865e4c5362eed40410d955' THEN 0.00  
 WHEN order_id = '678692cc2583002a9a42f608' THEN 0.00  
 WHEN order_id = '67866bd05362eed40410d9b9' THEN 0.00  
 WHEN order_id = '6788203e2583002a9a42fc1e' THEN 0.00  
 WHEN order_id = '6787d94b5362eed40410df41' THEN 0.00  
 WHEN order_id = '678805352583002a9a42fb85' THEN 0.00  
 WHEN order_id = '6788e90a5362eed40410e435' THEN 0.00  
 WHEN order_id = '6789660f46dd05f18e97ab1b' THEN 0.00  
 WHEN order_id = '67897626e4dd3a64d0d31119' THEN 0.00  
 WHEN order_id = '678970a2e4dd3a64d0d310ff' THEN 0.00  
 WHEN order_id = '678abd0e7958f9ced43ba45e' THEN 0.00  
 WHEN order_id = '678ac3e67958f9ced43ba483' THEN 0.00  
 WHEN order_id = '678aaf15e4dd3a64d0d3193f' THEN 0.00  
 WHEN order_id = '6740d51a4bd146113e694424' THEN 0.00  
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
        WHEN order_id = '675c69e52e1d04dfe94289c5' THEN -862.52
        WHEN order_id = '6776e4f7dfbe6dd933205c82' THEN 0.0  
         WHEN order_id = '677270e67104bc79f2a8ac97' THEN 106.79
         WHEN order_id = '677e7b72dfbe6dd93320672e' THEN 0.0  
         WHEN order_id = '677eafa37104bc79f2a8bb6c' THEN 0.0  
         WHEN order_id = '677eb6a4dfbe6dd933206831' THEN 0.0  
         WHEN order_id = '677edff8dfbe6dd9332068d6' THEN 0.0  
         WHEN order_id = '677ed8b3dfbe6dd9332068b6' THEN 0.0  
         WHEN order_id = '677ed4487104bc79f2a8bc04' THEN 0.0  
         WHEN order_id = '677ed6727104bc79f2a8bc12' THEN 0.0  
         WHEN order_id = '677ee0d67104bc79f2a8bc29' THEN 0.0  
         WHEN order_id = '677fd23a7104bc79f2a8c055' THEN 0.0  
         WHEN order_id = '677fe6597104bc79f2a8c0e8' THEN 0.0  
         WHEN order_id = '6761acb706c420992f837ae0' THEN -6677.27 
         WHEN order_id = '6759f0a917c167b3cf1d5bad' THEN 25.25  
         WHEN order_id = '675b02d4781d3609c1e97225' THEN 6.97  
         WHEN order_id = '675c67042e1d04dfe942898d' THEN 6.08  
         WHEN order_id = '675af7b808433ed244d897e9' THEN 17.14  
         WHEN order_id = '675c6ea3efa9f41236d87427' THEN 9.16  
         WHEN order_id = '675c7411efa9f41236d87439' THEN 9.15  
         WHEN order_id = '675c3d6b3703512d44aaa8e0' THEN 64.49  
         WHEN order_id = '675c462f3703512d44aaa91b' THEN 8.53  
         WHEN order_id = '67606db7f7660794452fa237' THEN 8.61  
         WHEN order_id = '67617c09f7660794452fa7f0' THEN 14.45  
         WHEN order_id = '6760651206c420992f837371' THEN 26.06  
         WHEN order_id = '67602b4a2e7d9061a2ee3b4b' THEN 11.39  
         WHEN order_id = '6762d82cf7660794452faf52' THEN 5.04  
         WHEN order_id = '676180dcf7660794452fa831' THEN 87.65  
         WHEN order_id = '6765968e7c2acf46df9a3a82' THEN 8.62  
         WHEN order_id = '675b40fb3703512d44aaa412' THEN -859.6  
         WHEN order_id = '6769b2e66cc818d350e04793' THEN 1444.6  
         WHEN order_id = '676ab681270b1f328d8f7cc2' THEN -3057.86  
         WHEN order_id = '6784f1bd7294db47e074844d' THEN 0.00  
         WHEN order_id = '6781628b54880f4bf013c566' THEN 0.00  
         WHEN order_id = '6781672b4f74fe5dab4f0cbc' THEN 0.00  
         WHEN order_id = '67815ca154880f4bf013c533' THEN 0.00  
         WHEN order_id = '6784eb1af7e293bf7267b519' THEN 0.00  
         WHEN order_id = '678567232583002a9a42efa2' THEN 0.00  
         WHEN order_id = '6785603c5362eed40410d47c' THEN 0.00  
         WHEN order_id = '67855e342583002a9a42ef84' THEN 0.00  
         WHEN order_id = '67868f632583002a9a42f5f6' THEN 0.00  
         WHEN order_id = '67865e4c5362eed40410d955' THEN 0.00  
         WHEN order_id = '678692cc2583002a9a42f608' THEN 0.00  
         WHEN order_id = '67866bd05362eed40410d9b9' THEN 0.00  
         WHEN order_id = '6788203e2583002a9a42fc1e' THEN 0.00  
         WHEN order_id = '6787d94b5362eed40410df41' THEN 0.00  
         WHEN order_id = '678805352583002a9a42fb85' THEN 0.00  
         WHEN order_id = '6788e90a5362eed40410e435' THEN 0.00  
         WHEN order_id = '6789660f46dd05f18e97ab1b' THEN 0.00  
         WHEN order_id = '67897626e4dd3a64d0d31119' THEN 0.00  
         WHEN order_id = '678970a2e4dd3a64d0d310ff' THEN 0.00  
         WHEN order_id = '678abd0e7958f9ced43ba45e' THEN 0.00  
         WHEN order_id = '678ac3e67958f9ced43ba483' THEN 0.00  
         WHEN order_id = '678aaf15e4dd3a64d0d3193f' THEN 0.00  
         WHEN order_id = '6740d51a4bd146113e694424' THEN 0.00  
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
