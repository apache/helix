# Report pass or fail
#   Input:
#      stat_txt, what to display to report 
# output $?
stat=$?
num_steps=$(($num_steps+1))
if [ $stat == 0 ]; then
  echo $stat_txt SUCCESS
else
  echo $stat_txt FAIL
 all_stat=$(($all_stat+1))
fi
if [ $final_report ]; then
   if [ $all_stat -gt 0 ]; then
     echo test FAIL. There are $all_stat failing steps.
   else
     if [ $num_steps -gt 1 ]; then
       echo ALL $num_steps steps for test SUCCESS.
     fi
   fi
fi

