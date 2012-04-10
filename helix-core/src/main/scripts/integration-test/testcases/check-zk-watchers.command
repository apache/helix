#/bin/bash

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

echo There are $# arguments to $0: $*
if [ "$#" -eq 2 ]; then
  zk_host=$1
  zk_port=$2
else
  echo "Usage: ./check-zk-wathcers zk_host zk_port"
  exit
fi

cons_file="/tmp/cons.$zk_host"
wchp_file="/tmp/wchp.$zk_host"
echo cons | nc $zk_host $zk_port > $cons_file
echo wchp | nc $zk_host $zk_port > $wchp_file

count=0
total_watch_count=0
while read line; do
  let count++
#  echo "$count: $line"
  session_id=`echo $line | awk 'BEGIN {FS="[(,)]"};{print $5}'`
#  echo "$session_id"
  if [ ! -z "$session_id" ]; then
    session_id=`echo $session_id | awk 'BEGIN {FS="[=]"};{print $2}'`
    watch_count=`grep $session_id $wchp_file | wc -l`
    echo "$session_id: $watch_count"
    total_watch_count=`expr $total_watch_count + $watch_count`
  fi
done < $cons_file
echo "total watcher count: $total_watch_count"


