{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

SELECT
"lina.ro@joompro-support.com" AS email,
"Sales" AS role,
"" AS name
UNION ALL 
SELECT
"a.poverinov@joompro-support.com" AS email,
"Support" AS role,
"" AS name
UNION ALL 
SELECT
"ekaterina.chervinskaya@joompro-support.com" AS email,
"Sales" AS role,
"" AS name
UNION ALL 
SELECT
"e.ershova@joompro-support.com" AS email,
"Support" AS role,
"" AS name
UNION ALL 
SELECT
"konstantin.gorbunov@joompro-support.com" AS email,
"Sales" AS role,
"" AS name
UNION ALL 
SELECT
"lubetskiy@joom.com" AS email,
"KAM" AS role,
"" AS name
UNION ALL 
SELECT
"sukhov@joom.com" AS email,
"KAM" AS role,
"" AS name
UNION ALL 
SELECT
"k.klevakina@joom.com" AS email,
"KAM" AS role,
"" AS name
UNION ALL 
SELECT
"m.fedotov@joom.com" AS email,
"Head KAM" AS role,
"" AS name
UNION ALL 
SELECT
"obiketova@joom.com" AS email,
"KAM" AS role,
"" AS name
UNION ALL 
SELECT
"evgenii.gashkov@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"d.kuraev@joom.com" AS email,
"KAM" AS role,
"" AS name
UNION ALL 
SELECT
"ratner@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"s.perekupko@joompro-support.com" AS email,
"Sales" AS role,
"" AS name
UNION ALL 
SELECT
"julia.julia11182@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"mpivovarova@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"dubravsky@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"banar@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"alexander.turchin@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"nyuvkina@joom.com" AS email,
"KAM" AS role,
"" AS name
UNION ALL 
SELECT
"ddgrishkin@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"w5346c@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"lergassy@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL 
SELECT
"leutsky@joom.com" AS email,
"Dev" AS role,
"" AS name
UNION ALL
SELECT
"a.nesterov@joompro-support.com" AS email,
"CallCenter" AS role,
"Иван Дергачёв" AS name
UNION ALL
SELECT 
"les.andrey@joom.com" AS email,
"Dev" AS role,
"" as name
UNION ALL
SELECT
"ivan.dergachev@joompro-support.com" AS email,
"CallCenter" AS role,
"Иван Дергачёв" AS name
UNION ALL 
SELECT 
"a.solomin@joompro-support.com" AS email,
"CallCenter" AS role,
"Александр Соломин" AS name
