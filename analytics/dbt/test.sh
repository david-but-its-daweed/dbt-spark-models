#!/bin/sh
set -euo pipefail

DBT_DIR=`pwd`
source $DBT_DIR/infra/functions.sh

# We use `git archive` command internally, which takes only committed changes
cecho "YELLOW" "WARNING: Ensure that you have committed all the changes required for deploy"

ANALYTICS_PROJECT_DIR=$(builtin cd $DBT_DIR/..; pwd)
PYTHON_DIR=$ANALYTICS_PROJECT_DIR/python
DBT_ETL_JOB_DIR=$ANALYTICS_PROJECT_DIR/joom/jobs/platform_team/thrift-server

# Produce target/manifest.json
cd $DBT_DIR
dbt compile --profiles-dir deploy/profiles --target prod --vars '{"start_date_ymd": "{start_date}", "end_date_ymd": "{end_date}"}'
WRONG_FILES=`find ./models/ -type f | grep -v .sql | grep -v yml | grep -v gitkeep`
WRONG_FILES_COUNT=`find ./models/ -type f | grep -v .sql | grep -v yml | grep -v gitkeep | wc -l`
echo "Found $WRONG_FILES_COUNT"
if [ $WRONG_FILES_COUNT -ne 0 ]; then
        echo "Found $WRONG_FILES_COUNT files with wrong filename: \n$WRONG_FILES"
        exit 1
fi
