#!/bin/sh
DBT_DIR=`pwd`
ANALYTICS_PROJECT_DIR=$(builtin cd $DBT_DIR/..; pwd)
PYTHON_DIR=$ANALYTICS_PROJECT_DIR/python
DBT_ETL_JOB_DIR=$ANALYTICS_PROJECT_DIR/joom/jobs/platform_team/thrift-server

# Produce target/manifest.json
cd $DBT_DIR
dbt compile --profiles-dir deploy/profiles --target prod --vars '{"start_date_ymd": "{start_date}", "end_date_ymd": "{end_date}"}'

# Produce job-graph.sh and copy it to Airflow DAG
cd $PYTHON_DIR
python3.8 tools/dbt_integration/mk_dbt_model_graph.py \
  --dbt-dir ../dbt \
  --output $DBT_ETL_JOB_DIR/src/main/airflow/jobs_graph.json

cd $DBT_ETL_JOB_DIR
$ANALYTICS_PROJECT_DIR/gradlew deployAirflow
