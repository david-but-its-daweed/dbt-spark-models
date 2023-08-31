#/bin/bash

set -eo pipefail

dbt compile --profiles-dir deploy/profiles --target prod --vars '{"start_date_ymd": "{start_date}", "end_date_ymd": "{end_date}", "table_name": "{table_name}"}'
python3 ./get_no_owner.py
