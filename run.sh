#!/bin/bash

python3 -m dbttools.run_dev \
  --profile=spark \
  --changed-only \
  --prod-profiles-dir=production/profiles \
  $@