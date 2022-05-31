#!/bin/sh
dbt compile --profiles-dir deploy/profiles --target prod --vars '{"start_date_ymd": "{start_date}", "end_date_ymd": "{end_date}"}'
