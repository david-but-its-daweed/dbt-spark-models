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
DBT_VARS="{'start_date_ymd':'$DEFAULT_START_DATE','end_date_ymd':'$DEFAULT_END_DATE','table_name':'gburg.test'}"


# Parse arguments
DROP=false
DRY_RUN=false
ARGS=()

for arg in "$@"; do
    case "$arg" in
        --drop) DROP=true ;;
        --drop-dry-run) DRY_RUN=true ;;
        *) ARGS+=("$arg") ;;  # args for DBT
    esac
done


# drop junk tables if requested
if [ "$DROP" = true ]; then
    if [ "$DRY_RUN" = true ]; then
        python3 drop_junk_tables.py "${ARGS[@]}" --dry-run
    else
        python3 drop_junk_tables.py "${ARGS[@]}"
    fi
fi


# run dbt
dbt run --vars $DBT_VARS "${ARGS[@]}"

