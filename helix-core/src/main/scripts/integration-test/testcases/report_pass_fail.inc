# Report pass or fail
#   Input:
#      stat_txt, what to display to report 
# output $?
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
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

