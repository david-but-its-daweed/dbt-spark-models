set -eo pipefail

d=2022-06-06
while [ "$d" != 2022-06-28 ]; do
  echo $d
  dbt run --profiles-dir localenv/profiles --vars "{\"start_date_ymd\": \"$d\"}" --select spark.mart.product_rating_segment
  d=$(date -I -d "$d + 1 day")
done