#!/bin/bash
# users/machines/dirs info for each test machine
USER_TAB=( "zzhang" "zzhang" "zzhang" )
# MACHINE_TAB=( "eat1-app20.corp" "eat1-app21.corp" "eat1-app22.corp" )
MACHINE_TAB=( "eat1-app26.corp" "eat1-app27.corp" "eat1-app28.corp" )

# SCRIPT_DIR_TAB=( "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" )

# constants
machine_nb=${#MACHINE_TAB[*]}

# colorful echo
red='\e[00;31m'
green='\e[00;32m'
function cecho
{
  message="$1"
  if [ -n "$message" ]; then
    color="$2"
    if [ -z "$color" ]; then
      echo "$message"
    else
      echo -e "$color$message\e[00m"
    fi
  fi
}

# : <<'END'
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 zklog_dir"
  exit
fi

../../../../../target/helix-core-pkg/bin/zk-log-analyzer $1 test-cluster localhost:2191

# END
