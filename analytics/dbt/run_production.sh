#!/bin/bash
set -euo pipefail

source infra/functions.sh

DBT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

# Interoperable btw Linux and OSX version of `date -d "<param> day ago" '+%Y-%m-%d'`
function days_ago {
PYTHON_AGO_ARG="$1" python3 - <<END
import os
from datetime import datetime, timedelta
ago = int(os.environ['PYTHON_AGO_ARG'])
ago_date = datetime.now() - timedelta(ago)
print(ago_date.strftime('%Y-%m-%d'))
END
}

DEFAULT_START_DATE=$(days_ago 1)
DEFAULT_END_DATE=$(days_ago 0)
DBT_VARS="{'start_date_ymd':'$DEFAULT_START_DATE','end_date_ymd':'$DEFAULT_END_DATE','table_name':'table_name'}"

dbt run --profiles-dir production/adhoc --vars $DBT_VARS $@
