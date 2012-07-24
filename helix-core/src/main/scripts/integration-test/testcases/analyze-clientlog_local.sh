#!/bin/bash
cd ../var/log/helix_random_kill_local

OPERATION_TAB=( "getData" "getData_async" "exists" "exists_async" "getChildren" "setData" "setData_async" "create" "create_async" "delete" "delete_async" )
PROPERTY_TAB=( "CURRENTSTATES" "EXTERNALVIEW" "MESSAGES" "STATEMODELDEFS" "HEALTHREPORT" "IDEALSTATES" "LIVEINSTANCES" "CONFIGS" "STATUSUPDATES" "CONTROLLER" )

# participant stats
echo "participant zk op stats:"
ls *process_start*.log -1rt | tail -n 1
echo -e "operation\t count\t sum (ms)\t avg (ms)"
echo -e "--------------------------------------------------"

op_nb=${#OPERATION_TAB[*]}
for j in `seq 0 $(($op_nb-1))`; do
  # append ,
  echo -ne "${OPERATION_TAB[$j]} "
  str=${OPERATION_TAB[$j]}","
  ls *process_start*.log -1rt | tail -n 1 | xargs grep $str | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++} END {if (count > 0) print "\t "  count "\t " sum/1e6 "\t " sum/count/1e6; else print "\t " 0}'

  property_nb=${#PROPERTY_TAB[*]}
  for i in `seq 0 $(($property_nb-1))`; do
    property=${PROPERTY_TAB[$i]}
    echo -ne "    $property"
    ls *process_start*.log -1rt | tail -n 1 | xargs grep $str | grep $property |sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++} END {if (count > 0) print "\t " count "\t " sum/1e6 "\t " sum/count/1e6; else print "\t " 0}'
  done

  echo ""
done

# controller stats
echo -e "\ncontroller zk op stats:"
ls *manager_start*.log -1rt | tail -n 1
echo -e "operation\t count\t sum\t avg"
echo -e "----------------------------------------------------"

for j in `seq 0 $(($op_nb-1))`; do
  # append ,
  echo -ne "${OPERATION_TAB[$j]} "
  str=${OPERATION_TAB[$j]}","
  ls *manager_start*.log -1rt | tail -n 1 | xargs grep $str | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++} END {if (count > 0) print "\t "  count "\t " sum/1e6 "\t " sum/count/1e6; else print "\t " 0}'

  for i in `seq 0 $(($property_nb-1))`; do
    property=${PROPERTY_TAB[$i]}
    echo -ne "    $property"
    ls *manager_start*.log -1rt | tail -n 1 | xargs grep $str | grep $property |sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++} END {if (count > 0) print "\t " count "\t " sum/1e6 "\t " sum/count/1e6; else print "\t " 0}'
  done

  echo ""
done

