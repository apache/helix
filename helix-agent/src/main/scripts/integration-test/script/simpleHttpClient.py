#!/usr/bin/env python
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


import httplib
import sys

#get http server ip
http_server = "localhost:8080" 
#create a connection
conn = httplib.HTTPConnection(http_server)

#request command to server
if len(sys.argv) > 1:
    conn.request("GET", "/" + sys.argv[1])
else:
    conn.request("GET", "/")

#get response from server
rsp = conn.getresponse()

#print server response and data
#print(rsp.status, rsp.reason)
data_received = rsp.read()
print(data_received)
    
conn.close()
sys.exit(0)
