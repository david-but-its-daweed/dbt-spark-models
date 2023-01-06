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

USER_NAME=$(whoami)
DEFAULT_START_DATE=$(days_ago 1)
DEFAULT_END_DATE=$(days_ago 0)
DBT_VARS="{'start_date_ymd':'$DEFAULT_START_DATE','end_date_ymd':'$DEFAULT_END_DATE'}"

if command -v python3 >/dev/null 2>&1; then
    PYTHON3_AVAILABLE=true
else
    cecho "RED" "Oops, Python 3 is not available. Please install or activate it."
    exit 1
fi

if command dbt --version >/dev/null 2>&1; then
    PYTHON3_AVAILABLE=true
else
    cecho "YELLOW" "DBT is missing. Installing..."
    pip3 install dbt-core==1.3.0 dbt-spark[PyHive]==1.3.0
fi


PROFILES_DIR=${DBT_DIR}/localenv/profiles
PROFILES_YML=${PROFILES_DIR}/profiles.yml

if test -f "$PROFILES_YML"; then
  CHANGE_PROFILES_YML=false
else
    CHANGE_PROFILES_YML=true
    mkdir -p $(dirname "${PROFILES_YML}")
    cp infra/profiles.yml.template ${PROFILES_YML}
fi

# Create logs directory, because we use it for thrift server logs.
mkdir -p ${DBT_DIR}/logs
WHOAMI_CLEAN=$(whoami | sed 's/[^a-zA-Z0-9]//g')
JUNK_DATABASE_DEFAULT="junk_${WHOAMI_CLEAN}"
if [ "$CHANGE_PROFILES_YML" = true ]; then
    read -p "Specify your personal junk database name [${JUNK_DATABASE_DEFAULT}]: " JUNK_DATABASE
    JUNK_DATABASE=${JUNK_DATABASE:-$JUNK_DATABASE_DEFAULT}

    file_replace_first "^(\s*)(schema\s*:\s*[a-zA-Z0-9_]*\s*$)" "\1schema: ${JUNK_DATABASE}" ${PROFILES_YML}
    cecho "GREEN" "Successfully updated 'schema' option in ${PROFILES_YML} into '${JUNK_DATABASE}'"
fi

dbt run --profiles-dir $PROFILES_DIR --vars $DBT_VARS $@
