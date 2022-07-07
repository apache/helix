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

<head>
  <title>Tutorial - Task Throttling</title>
</head>

## [Helix Tutorial](./Tutorial.html): Task Throttling

In this chapter, we\'ll learn how to control the parallel execution of tasks in the task framework.

### Task Throttling Configuration

Helix can control the number of tasks that are executed in parallel according to multiple thresholds.
Applications can set these thresholds in the following configuration items:

* JobConfig.ConcurrentTasksPerInstance The number of concurrent tasks in this job that are allowed to run on an instance.
* InstanceConfig.MAX_CONCURRENT_TASK The number of total concurrent tasks that are allowed to run on an instance.

Also see [WorkflowConfig.ParallelJobs](./tutorial_task_framework.html).

### Job Priority for Task Throttling

Whenever there are too many tasks to be scheduled according to the threshold, Helix will prioritize the older jobs.
The age of a job is calculated based on the job start time.
