{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

select "629898f122c4056c920f47da" as id
	, 2 as order_channel
	, "AeroMtl" as crowdin_key
	, "ATC_TANAIS_TANAIS" as name
	, 5 as min_days
	, 8 as max_days
	, "Aero" as channel_type
union all
select "629898f122c4056c920f47db" as id
	, 2 as order_channel
	, "AeroMtl" as crowdin_key
	, "ATC_HECNY_R_HECNY_R" as name
	, 5 as min_days
	, 8 as max_days
	, "Aero" as channel_type
union all
select "629898f122c4056c920f47dc" as id
	, 1 as order_channel
	, "AutoMtl" as crowdin_key
	, "HECNY_R_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47de" as id
	, 1 as order_channel
	, "AutoMtl" as crowdin_key
	, "AKFA_TANAIS_TANAIS" as name
	, 28 as min_days
	, 32 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47df" as id
	, 1 as order_channel
	, "AutoMtl" as crowdin_key
	, "AKFA_HECNY_R_HECNY_R" as name
	, 28 as min_days
	, 32 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ea" as id
	, 2 as order_channel
	, "TrainMtl" as crowdin_key
	, "YMTRANS_TANAIS_TANAIS" as name
	, 25 as min_days
	, 30 as max_days
	, "Rail" as channel_type
union all
select "629898f122c4056c920f47eb" as id
	, 2 as order_channel
	, "TrainMtl" as crowdin_key
	, "YMTRANS_HECNY_R_HECNY_R" as name
	, 25 as min_days
	, 30 as max_days
	, "Rail" as channel_type
union all
select "629898f122c4056c920f47ec" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "CEL_TANAIS_TANAIS" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ed" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "CEL_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ee" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "KIM_TANAIS_TANAIS" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ef" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "KIM_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47fa" as id
	, 2 as order_channel
	, "Sea" as crowdin_key
	, "PARUS_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Sea" as channel_type
union all
select "629898f122c4056c920f47fb" as id
	, 2 as order_channel
	, "Sea" as crowdin_key
	, "PARUS_TANAIS_TANAIS" as name
	, 20 as min_days
	, 24 as max_days
	, "Sea" as channel_type
union all
select "629898f122c4056c920f47da" as id
	, 2 as order_channel
	, "AeroMtl" as crowdin_key
	, "ATC_TANAIS_TANAIS" as name
	, 5 as min_days
	, 8 as max_days
	, "Aero" as channel_type
union all
select "629898f122c4056c920f47db" as id
	, 2 as order_channel
	, "AeroMtl" as crowdin_key
	, "ATC_HECNY_R_HECNY_R" as name
	, 5 as min_days
	, 8 as max_days
	, "Aero" as channel_type
union all
select "629898f122c4056c920f47dc" as id
	, 1 as order_channel
	, "AutoMtl" as crowdin_key
	, "HECNY_R_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47de" as id
	, 1 as order_channel
	, "AutoMtl" as crowdin_key
	, "AKFA_TANAIS_TANAIS" as name
	, 28 as min_days
	, 32 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47df" as id
	, 1 as order_channel
	, "AutoMtl" as crowdin_key
	, "AKFA_HECNY_R_HECNY_R" as name
	, 28 as min_days
	, 32 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ea" as id
	, 2 as order_channel
	, "TrainMtl" as crowdin_key
	, "YMTRANS_TANAIS_TANAIS" as name
	, 25 as min_days
	, 30 as max_days
	, "Rail" as channel_type
union all
select "629898f122c4056c920f47eb" as id
	, 2 as order_channel
	, "TrainMtl" as crowdin_key
	, "YMTRANS_HECNY_R_HECNY_R" as name
	, 25 as min_days
	, 30 as max_days
	, "Rail" as channel_type
union all
select "629898f122c4056c920f47ec" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "CEL_TANAIS_TANAIS" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ed" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "CEL_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ee" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "KIM_TANAIS_TANAIS" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47ef" as id
	, 2 as order_channel
	, "AutoMtl" as crowdin_key
	, "KIM_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Auto" as channel_type
union all
select "629898f122c4056c920f47fa" as id
	, 2 as order_channel
	, "Sea" as crowdin_key
	, "PARUS_HECNY_R_HECNY_R" as name
	, 20 as min_days
	, 24 as max_days
	, "Sea" as channel_type
union all
select "629898f122c4056c920f47fb" as id
	, 2 as order_channel
	, "Sea" as crowdin_key
	, "PARUS_TANAIS_TANAIS" as name
	, 20 as min_days
	, 24 as max_days
	, "Sea" as channel_type
