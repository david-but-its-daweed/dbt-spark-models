#!/bin/bash

echo "UNDER DEVELOPMENT"
exit()

python3 -m dbttools.run_dev \
  --profile=spark \
  --changed-only \
  --prod-profiles-dir=production/profiles \
  $@