#!/usr/bin/python
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

import os, time, sys

this_file_full_path=os.path.abspath(__file__)
this_file_dirname=os.path.dirname(this_file_full_path)

# write pid for monitor
pid=os.getpid()

file=open(os.path.join(this_file_dirname,"../var/log/default/foo_TestDB0_0_pid.txt"), "wb")
file.write("%s\n" % os.getpid())
file.close()

# this output tells dds_driver.py to return
print "start"
sys.stdout.flush()

print "byebye"

