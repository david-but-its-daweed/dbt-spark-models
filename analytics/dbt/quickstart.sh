#!/bin/sh
set -euo pipefail
IFS=$'\n\t'

cat infra/logo.txt
source infra/functions.sh

cecho "YELLOW" "Checking the environment..."

PYTHON3_AVAILABLE=false
JAVA11_AVAILABLE=fales

if command -v python3 >/dev/null 2>&1; then
    cecho "GREEN" "Python 3 is available."
    PYTHON3_AVAILABLE=true
else
    cecho "RED" "Oops, Python 3 is not available. Please install or activate it."
fi

if type -p java >/dev/null 2>&1; then
    if [ $(javap -verbose java.lang.String | grep "major version" | cut -d " " -f5) -ge "55" ]; then
        cecho "GREEN" "Java 11 or later is available."
        JAVA11_AVAILABLE=true
    else
        cecho "RED" "Java is installed, but has an outdated version. Please, upgrade it manually or using https://sdkman.io"
    fi
else
    cecho "RED" "Java is not installed. Please, install it manually or using https://sdkman.io"
fi

if [ "$PYTHON3_AVAILABLE" = false ] || [ "$JAVA11_AVAILABLE" = false ]; then
    exit 1
fi

CHANGE_PROFILES_YML=true

DBT_DIR=`pwd`
PROFILES_YML=${DBT_DIR}/localenv/profiles/profiles.yml
if test -f "$PROFILES_YML"; then
    cecho "YELLOW" "$PROFILES_YML already exists, would you like to change settings?."
    select yn in "Yes" "No"; do
        case $yn in
            Yes ) break;;
            No ) CHANGE_PROFILES_YML=false; break;;
            Exit ) exit;;
        esac
    done
else
    mkdir -p $(dirname "${PROFILES_YML}")
    cp infra/profiles.yml.template ${PROFILES_YML}
fi

# Create logs directory, because we use it for thrift server logs.
mkdir -p ${DBT_DIR}/logs

WHOAMI_CLEAN=$(whoami | sed 's/[^a-zA-Z0-9]//g')
# TODO: Extract JUNK_DATABASE from profiles.yml if it already exists and CHANGE_PROFILES_YML = false
JUNK_DATABASE="junk_${WHOAMI_CLEAN}"
if [ "$CHANGE_PROFILES_YML" = true ]; then
    cecho "YELLOW" "Specify your personal junk database name ('${JUNK_DATABASE}' is suggested for you)."
    read JUNK_DATABASE

    file_replace_first "^(\s*)(schema\s*:\s*[a-zA-Z0-9_]*\s*$)" "\1schema: ${JUNK_DATABASE}" ${PROFILES_YML}
    cecho "GREEN" "Successfully updated 'schema' option in ${PROFILES_YML} into '${JUNK_DATABASE}'"
fi

JOOMDBT_HOME=${HOME}/.joomdbt
cecho "YELLOW" "Specify home directory for artifacts. Press Enter to use suggested path ${JOOMDBT_HOME}"
read JOOMDBT_HOME_OVERRIDE
JOOMDBT_HOME=${JOOMDBT_HOME_OVERRIDE:-${JOOMDBT_HOME}}

JOOMDBT_HOME_RECREATE=true
if test -d "$JOOMDBT_HOME"; then
    cecho "YELLOW" "$JOOMDBT_HOME already exists, would you like to recreate it from scratch?"
    select yn2 in "Yes" "No"; do
        case $yn2 in
            Yes ) rm -rf ${JOOMDBT_HOME}; break;;
            No ) JOOMDBT_HOME_RECREATE=false; break;;
            Exit ) exit;;
        esac
    done
fi

SQLLINE_HOME=${JOOMDBT_HOME}/sqlline

if [ "$JOOMDBT_HOME_RECREATE" = true ]; then
    mkdir -p ${JOOMDBT_HOME}
    mkdir -p ${SQLLINE_HOME}

    cecho "YELLOW" "Downloading sqlline and JDBC driver..."
    wget https://repo1.maven.org/maven2/sqlline/sqlline/1.12.0/sqlline-1.12.0-jar-with-dependencies.jar -P ${SQLLINE_HOME}
    wget https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar -P ${SQLLINE_HOME}
fi

SHOULD_RUN_THRIFT_SERVER=false
cecho "YELLOW" "Would you like to start a Thrift Server and check everything is allright?"
select should_run_thrift_select in "Yes" "No"; do
    case $should_run_thrift_select in
        Yes ) SHOULD_RUN_THRIFT_SERVER=true; break;;
        No ) break;;
    esac
done

if [ "$SHOULD_RUN_THRIFT_SERVER" = true ]; then
    DBT_DIR=`pwd`
    ANALYTICS_PROJECT_DIR=$(builtin cd $DBT_DIR/..; pwd)
    DBT_ETL_JOB_DIR=$ANALYTICS_PROJECT_DIR/joom/jobs/platform_team/thrift-server
    LOG_DIR=${DBT_DIR}/logs
    THRIFT_SERVER_LOG_FILE_STDOUT=${LOG_DIR}/thrift-server-quickstart.out
    THRIFT_SERVER_LOG_FILE_STDERR=${LOG_DIR}/thrift-server-quickstart.log
    THRIFT_SERVER_PORT=10000

    # Emptying logs, because later we'll read info about profiles.yml file change to know Thrift server is up and running
    : > ${THRIFT_SERVER_LOG_FILE_STDOUT}
    : > ${THRIFT_SERVER_LOG_FILE_STDERR}

    cd $DBT_ETL_JOB_DIR

    submit_job_save_ip \
        $ANALYTICS_PROJECT_DIR'/gradlew start -Pargs="--user '$JUNK_DATABASE' --terminate-after-secs 600"' \
        $PROFILES_YML 2> ${THRIFT_SERVER_LOG_FILE_STDERR} 1> ${THRIFT_SERVER_LOG_FILE_STDOUT} &

    wait_specific_line_in_file $THRIFT_SERVER_LOG_FILE_STDOUT "driver_pod_ip:" 600
    THRIFT_DERVER_DRIVER_POD_IP=$(echo $wait_specific_line_in_file_result | cut -d':' -f2)

    wait_host_port_is_open ${THRIFT_DERVER_DRIVER_POD_IP} ${THRIFT_SERVER_PORT} 600

    THRIFT_SERVER_URL=${THRIFT_DERVER_DRIVER_POD_IP}:${THRIFT_SERVER_PORT}

    CHECKS_SUCCEEDED=false

    if sqlline_database_exists $THRIFT_SERVER_URL $JUNK_DATABASE; then
        cecho "GREEN" "Database '$JUNK_DATABASE' exists!"
    else
        cecho "YELLOW" "Database '$JUNK_DATABASE' does not exist, creating.."
        sqlline_exec $THRIFT_SERVER_URL "create database $JUNK_DATABASE location 's3://joom-analytics-users/$JUNK_DATABASE'"
    fi

    sqlline_get_database_location $THRIFT_SERVER_URL $JUNK_DATABASE
    JUNK_DATABASE_LOCATION=$sqlline_get_database_location_result

    if [[ $JUNK_DATABASE_LOCATION == s3://* ]]; then
        cecho "GREEN" "Database '$JUNK_DATABASE' points to correct S3 location"
    else
        cecho "RED" "Database '$JUNK_DATABASE' points to non-S3 location '$JUNK_DATABASE_LOCATION'. Please recreate it with S3 location or use another database." 
        CHECKS_SUCCEEDED=false 
    fi

    if [ "$SHOULD_RUN_THRIFT_SERVER" = false ]; then
        cecho "RED" "Fix issues above and rerun this script again"
    else
        cecho "GREEN" "Congratulations, everything is set up! Now you can create and modify models and test your changes using ./run.sh"
    fi
fi

    