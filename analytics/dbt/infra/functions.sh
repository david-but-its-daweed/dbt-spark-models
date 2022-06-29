# Coloured echo command. Usage example: cecho "RED" "Message"
function cecho {
    RED="\033[0;31m"
    GREEN="\033[0;32m"
    YELLOW="\033[1;33m"
    # ... ADD MORE COLORS
    NC="\033[0m" # No Color
    # ZSH
    # printf "${(P)1}${2} ${NC}\n"
    # Bash
    printf "${!1}${2} ${NC}\n"
}

function days_ago {
PYTHON_AGO_ARG="$1" python3 - <<END
import os
from datetime import datetime, timedelta
ago = int(os.environ['PYTHON_AGO_ARG'])
ago_date = datetime.now() - timedelta(ago)
print(ago_date.strftime('%Y-%m-%d'))
END
}

# 
function file_replace_first {
FILE_REPLACE_FIRST_FROM_ARG="$1" FILE_REPLACE_FIRST_INTO_ARG="$2" FILE_REPLACE_FIRST_FILE_ARG="$3" python3 - <<END
import os
import re

from_string=os.environ['FILE_REPLACE_FIRST_FROM_ARG']
into_string=r'%s' % os.environ['FILE_REPLACE_FIRST_INTO_ARG']
file_name=os.environ['FILE_REPLACE_FIRST_FILE_ARG']

with open (file_name, 'r' ) as f:
    content = f.read()
    content_new = re.sub(from_string, into_string, content, flags = re.M)

with open (file_name, 'w' ) as f:
    f.write(content_new)
END
}

function submit_job_save_ip {
SUBMIT_JOB_SAVE_IP_CMD="$1"
SUBMIT_JOB_SAVE_IP_PROFILES_YML="$2"
eval ${SUBMIT_JOB_SAVE_IP_CMD} | while read y
do
  if [[ $y == *"\"driver_pod_ip\""* ]]; then
    echo $y 1>&2
    SUBMIT_JOB_SAVE_IP_DRIVER_POD_IP=$(echo $y | cut -d':' -f2 | tr -dc '0-9.\n')
    file_replace_first "^(\s*)(host\s*:.*$)" "\1host: ${SUBMIT_JOB_SAVE_IP_DRIVER_POD_IP}" ${SUBMIT_JOB_SAVE_IP_PROFILES_YML}
    echo "driver_pod_ip:${SUBMIT_JOB_SAVE_IP_DRIVER_POD_IP}"
  else
    echo $y 1>&2
  fi
done
}

function sqlline_exec {
  local url="$1"
  local cmd="$2"

  cecho "NC" "Running command '$cmd' against $url" 

  sqlline_exec_result=$(java \
    -cp ${SQLLINE_HOME}'/*' sqlline.SqlLine -n "" -p "" \
    -d org.apache.hive.jdbc.HiveDriver \
    -u jdbc:hive2://$url \
    -e $cmd \
    --outputformat=tsv \
  )
}

function sqlline_database_exists {
  local url="$1"
  local database_name="$2"

  sqlline_exec $url "!sql show databases"
  echo $sqlline_exec_result | grep -q -m1 $database_name 
  return $?
}

function sqlline_get_database_location {
  local url="$1"
  local database_name="$2"

  sqlline_exec $url "describe database $database_name"
  sqlline_get_database_location_result=$(echo "$sqlline_exec_result" | grep '"Location"' | cut -f2 |  tr -d '"')
}

function wait_host_port_is_open {
  local host="$1"
  local port="$2"
  local timeout="${3:-300}"

  cecho "NC" "Connecting to $host:$port"
  for _ in `seq 1 $timeout`; do
    printf '.'
    if nc -z $host $port; then
      cecho "GREEN" "\nSuccessfully connected to $host:$port"
      return 0
    fi
    sleep 1
  done
  cecho "RED" "\nTimed out waiting for $host:$port after $timeout seconds"
}

function wait_specific_line_in_file {
  local filename="$1"
  local pattern="$2"
  local timeout="${3:-300}"

  cecho "NC" "Waiting for pattern '$pattern' in $filename"
  for _ in `seq 1 $timeout`; do
    printf '.'
    if grep -q -m1 $pattern $filename; then
      cecho "GREEN" "\nFound pattern '$pattern' in $filename"
      wait_specific_line_in_file_result=$(grep -m1 $pattern $filename)
      return 0
    else
      sleep 1
    fi
  done
  cecho "RED" "\nTimed out waiting for pattern '$pattern' in $filename after $timeout seconds"
  return 1
}