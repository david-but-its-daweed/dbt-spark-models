#!/bin/bash

python3 -m dbttools.load_to_bq \
  --profile=spark \
  --profiles-dir=production/profiles \
  $@