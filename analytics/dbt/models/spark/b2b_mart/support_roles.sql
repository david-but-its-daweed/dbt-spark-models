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
"Sales" AS role
UNION ALL 
SELECT
"a.poverinov@joompro-support.com" AS email,
"Support" AS role
UNION ALL 
SELECT
"ekaterina.chervinskaya@joompro-support.com" AS email,
"Sales" AS role
UNION ALL 
SELECT
"e.ershova@joompro-support.com" AS email,
"Support" AS role
UNION ALL 
SELECT
"konstantin.gorbunov@joompro-support.com" AS email,
"Sales" AS role
UNION ALL 
SELECT
"lubetskiy@joom.com" AS email,
"KAM" AS role
UNION ALL 
SELECT
"sukhov@joom.com" AS email,
"KAM" AS role
UNION ALL 
SELECT
"k.klevakina@joom.com" AS email,
"KAM" AS role
UNION ALL 
SELECT
"m.fedotov@joom.com" AS email,
"KAM" AS role
UNION ALL 
SELECT
"evgenii.gashkov@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"d.kuraev@joom.com" AS email,
"KAM" AS role
UNION ALL 
SELECT
"ratner@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"s.perekupko@joompro-support.com" AS email,
"Sales" AS role
UNION ALL 
SELECT
"julia.julia11182@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"mpivovarova@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"dubravsky@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"banar@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"alexander.turchin@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"nyuvkina@joom.com" AS email,
"KAM" AS role
UNION ALL 
SELECT
"ddgrishkin@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"w5346c@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"lergassy@joom.com" AS email,
"Dev" AS role
UNION ALL 
SELECT
"leutsky@joom.com" AS email,
"Dev" AS role
UNION ALL
SELECT
"a.nesterov@joompro-support.com" AS email,
"CallCenter" AS role
UNION ALL
SELECT
"Ivan.dergachev@joompro-support.com" AS email,
"CallCenter" AS role
