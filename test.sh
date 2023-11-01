#!/bin/sh

DBT_DIR=`pwd`
# Produce target/manifest.json
cd $DBT_DIR
WRONG_FILES=`find ./models/ -type f | grep -v .sql | grep -v yml | grep -v gitkeep | grep -v \.md`
WRONG_FILES_COUNT=`find ./models/ -type f | grep -v .sql | grep -v yml | grep -v gitkeep | grep -v \.md | wc -l`
if [ $WRONG_FILES_COUNT -ne 0 ]; then
        echo "All filenames should end with .sql. Found $WRONG_FILES_COUNT files with wrong filename: \n$WRONG_FILES"
        exit 1
fi

dbt --debug compile --profiles-dir production/profiles --target prod --vars '{"start_date_ymd": "2020", "end_date_ymd": "2020", "table_name": "2020"}'
