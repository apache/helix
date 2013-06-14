#!/usr/bin/env python

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