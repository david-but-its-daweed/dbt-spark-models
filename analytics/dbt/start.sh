#!/bin/sh
DBT_DIR=`pwd`
ANALYTICS_PROJECT_DIR=$(builtin cd $DBT_DIR/..; pwd)
DBT_ETL_JOB_DIR=$ANALYTICS_PROJECT_DIR/joom/jobs/platform_team/thrift-server

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
DBT_SELECT="spark.junk2.dbt_test"
TERMINATE_AFTER_SECS=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --user)
      USER_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    --vars)
      DBT_VARS="$2"
      shift # past argument
      shift # past value
      ;;
    --select)
      DBT_SELECT="$2"
      shift # past argument
      shift # past value
      ;;
    --terminate-after-secs)
      TERMINATE_AFTER_SECS="$2"
      shift # past argument
      shift # past value
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

cd $DBT_ETL_JOB_DIR
$ANALYTICS_PROJECT_DIR/gradlew start -Pargs="--user $USER_NAME --dbt-vars $DBT_VARS --dbt-select $DBT_SELECT --terminate-after-secs $TERMINATE_AFTER_SECS"
