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

Distributed task execution
----------------------------
Quick Demo
==========
This recipe is intended to demonstrate how task dependencies can be modeled using primitives provided by Helix. A given task can be run with desired parallelism and will start
only when up-stream dependencies are met. The demo executes the task DAG described below using 10 workers. Although the demo starts the workers as threads, there is no requirement 
that all the workers need to run in the same process. In reality, these workers work on many different boxes on a cluster.  When worker failures occur, Helix takes care of 
re-assigning a failed task partition to a new worker. This recipe makes use of redis as a task result store. Any backing store can be used with a suitable implementation of 
TaskResultStore. The demo populates the task result store with 10000 dummy impression events and dummy click events based on a click probability of  0.01. It then submits the task 
execution DAG which results in execution of tasks with specified parallelism. In order to run the demo, use the following steps

```
Start redis e.g:
./redis-server --port 6379

git clone https://git-wip-us.apache.org/repos/asf/incubator-helix.git
cd recipes/task-execution
mvn clean install package -DskipTests
cd target/task-execution-pkg/bin
chmod +x task-exection-demo.sh
./task-exection-demo.sh 2181 localhost 6379

```



                       +-----------------+       +----------------+
                       |   filterImps    |       |  filterClicks  |
                       | (parallelism=10)|       | (parallelism=5)|
                       +----------+-----++       +-------+--------+
                       |          |     |                |
                       |          |     |                |
                       |          |     |                |
                       |          |     +------->--------v------------+
      +--------------<-+   +------v-------+    |  impClickJoin        |
      |impCountsByGender   |impCountsByCountry | (parallelism=10)     |
      |(parallelism=10)    |(parallelism=10)   ++-------------------+-+
      +-----------+--+     +---+----------+     |                   |
                  |            |                |                   |
                  |            |                |                   |
                  |            |       +--------v---------+       +-v-------------------+
                  |            |       |clickCountsByGender       |clickCountsByCountry |
                  |            |       |(parallelism=5)   |       |(parallelism=5)      |
                  |            |       +----+-------------+       +---------------------+
                  |            |            |                     |
                  |            |            |                     |
                  |            |            |                     |
                  +----->+-----+>-----------v----+<---------------+
                         | report                |
                         |(parallelism=1)        |
                         +-----------------------+


(credit for above ascii art: http://www.asciiflow.com)

