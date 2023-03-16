FROM ghcr.io/dbt-labs/dbt-spark:1.4.1

COPY . /dbt

WORKDIR /dbt

CMD ["dbt", "docs", "serve", "--profiles-dir", "production/adhoc"]
