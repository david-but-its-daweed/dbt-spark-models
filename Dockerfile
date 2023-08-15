FROM jfrog.joom.it/datasci/analytics/spark-dbt-client:latest

COPY dbt-models.tar.gz ./dbt-models.tar.gz
RUN tar -xvf dbt-models.tar.gz


RUN /bin/bash -c "dbt docs generate --profiles-dir production/adhoc --profile spark --vars '{'\''start_date_ymd'\'':'\''date_sub(current_date(), 1)'\'','\''end_date_ymd'\'':'\''current_date()'\'','\''table_name'\'':'\'''\''}'"
ENTRYPOINT ["dbt", "docs", "serve", "--profiles-dir", "production/adhoc"]