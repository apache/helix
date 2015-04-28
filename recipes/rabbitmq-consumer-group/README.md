<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Near real time rsync replicated file system
===========================================

Quickdemo
=========

* This demo starts 3 instances with id's as ```localhost_12001, localhost_12002, localhost_12003```
* Each instance stores its files under /tmp/<id>/filestore
* ``` localhost_12001 ``` is designated as the master and ``` localhost_12002 and localhost_12003``` are the slaves.
* Files written to master are replicated to the slaves automatically. In this demo, a.txt and b.txt are written to ```/tmp/localhost_12001/filestore``` and it gets replicated to other folders.
* When the master is stopped, ```localhost_12002``` is promoted to master. 
* The other slave ```localhost_12003``` stops replicating from ```localhost_12001``` and starts replicating from new master ```localhost_12002```
* Files written to new master ```localhost_12002``` are replicated to ```localhost_12003```
* In the end state of this quick demo, ```localhost_12002``` is the master and ```localhost_12003``` is the slave. Manually create files under ```/tmp/localhost_12002/filestore``` and see that appears in ```/tmp/localhost_12003/filestore```
* Ignore the interrupted exceptions on the console :-).

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd recipes/rsync-replicated-file-system/
mvn clean install package -DskipTests
cd target/rsync-replicated-file-system-pkg/bin
./quickdemo

```

See [rsync_replicated_file_store](http://helix.apache.org/recipes/rsync_replicated_file_store.html) for more information
