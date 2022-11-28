#!/bin/sh
set -eu pipefail

DBT_DIR=`pwd`
# Produce target/manifest.json
cd $DBT_DIR
WRONG_FILES=`find ./models/ -type f | grep -v .sql | grep -v yml | grep -v gitkeep`
WRONG_FILES_COUNT=`find ./models/ -type f | grep -v .sql | grep -v yml | grep -v gitkeep | wc -l`
if [ $WRONG_FILES_COUNT -ne 0 ]; then
        echo "Found $WRONG_FILES_COUNT files with wrong filename: \n$WRONG_FILES"
        exit 1
fi
