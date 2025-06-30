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

# Quota-based Task Scheduling


## Introduction

![Intro](./images/quota_intro.png)

Quota-based task scheduling is a feature addition to Helix Task Framework that enables users of Task Framework to apply the notion of categories in distributed task management.

## Purpose

As Helix Task Framework gains usage in other open-source frameworks such as [Apache Gobblin](https://gobblin.apache.org/) and [Apache Pinot](http://pinot.incubator.apache.org/), it has also seen an increase in the variety in the types of distributed tasks it was managing. There have also been explicit feature requests to Helix for differentiating different types of tasks by creating corresponding quotas. 

Quota-based task scheduling aims to fulfill these requests by allowing users to define a quota profile consisting of quota types and their corresponding quotas. The goal of this feature is threefold: 1) the user will have the ability to prioritize one type of workflows/jobs/tasks over another and 2) achieve isolation among the type of tasks and 3) make monitoring easier by tracking the status of distributed execution by type.



## Glossary and Definitions

-   Task Framework: a component of Apache Helix. A framework on which users can define and run workflows, jobs, and tasks in a distributed way.
-   Workflow: the largest unit of work in Task Framework. A workflow consists of one or more jobs. There are two types of workflows:
    -   Generic workflow: a generic workflow is a workflow consisting of jobs (a job DAG) that are used for general purposes. **A generic workflow may be removed if expired or timed out.**
    -   Job queue: a job queue is a special type of workflow consisting of jobs that tend to have a linear dependency (this dependency is configurable, however). **There is no expiration for job queues** - it lives on until it is deleted.
-   Job: the second largest unit of work in Task Framework. A job consists of one or more mutually independent tasks. There are two types of jobs:
    -   Generic job: a generic job is a job consisting of one or more tasks.
    -   Targeted job: a targeted job differs from generic jobs in that these jobs must have a  _target resource_, and the tasks belonging to such jobs will be scheduled alongside the partitions of the target resource. To illustrate, an Espresso user of Task Framework may wish to schedule a backup job on one of their DBs called  _MemberDataDB_. This DB will be divided into multiple partitions (_MemberDataDB_1, _MemberDataDB_2, ... _MemberDataDB_N)___, and suppose that a targeted job is submitted such that its tasks will be paired up with each of those partitions. This "pairing-up" is necessary because this task is a backup task that needs to be on the same physical machine as those partitions the task is backing up.
-   Task: the **smallest unit of work** in Task Framework. A task is an independent unit of work.
-   Quota resource type: denotes a particular type of resource. Examples would be JVM thread count, memory, CPU resources, etc.. Generally, each task that runs on a Helix Participant (= instance, worker, node) occupies a set amount of resources.  **Note that only JVM thread count is the only quota resource type currently supported by Task Framework, with each task occupying 1 thread out of 40 threads available per Helix Participant (instance).**
-   Quota type: denotes which category a given job and its underlying tasks should be classified as. For example, you may define a quota configuration with two quota types, type "Backup", and type "Aggregation" and a default type "DEFAULT". You may prioritize the backup type by giving it a higher quota ratio - such as 20:10:10, respectively. When there are streams of jobs being submitted, you can expect each Participant, assuming that it has a total of 40 JVM threads, will have 20 "Backup" tasks, 10 "Aggregation" tasks, and 10 "DEFAULT" tasks. **Quota types are defined and applied at the job level, meaning all tasks belonging to a particular job with a quota type will be of that quota type.** Note that if a quota type is set for a workflow, then all jobs belonging to that workflow will  _inherit_  the type from the workflow.
-   Quota: a number referring to a **relative ratio** that determines what portion of given resources should be allotted to a particular quota type.
    -   E.g.) TYPE_0:  40, TYPE_1:  20, ..., DEFAULT:  40
-   Quota config: a set of string-integer mappings that indicate the  quota resource type, quota types, and corresponding quotas. **Task Framework stores the quota config in ClusterConfig.**

## Architecture

### AssignableInstance

AssignableInstance is an abstraction that represents each live Participant that is able to take on tasks from the Controller. Each AssignableInstance will cache what tasks it has running  as well as remaining task counts from the quota-based capacity calculation.

### AssignableInstanceManager

AssignableInstanceManager manages all AssignableInstances. It also serves as a connecting layer between the Controller and each AssignableInstance. AssignableInstanceManager also provides a set of interfaces that allows the Controller to easily determine whether an AssignableInstance is able to take on more tasks.

### TaskAssigner

The TaskAssigner interface provides basic API methods that involve assignments of tasks based on quota constraints. Currently, Task Framework only concerns the number of Participant-side JVM threads, each of which corresponds to an active task.

### RuntimeJobDag (JobDagIterator)

This new component serves as an iterator for JobDAGs for the Controller. Previously, task assignment required the Controller to iterate through all jobs and their underlying tasks to determine whether there were any tasks that needed to be assigned and scheduled. This proved to be inefficient and did not scale with the increasing load we were putting on Task Framework. Each RuntimeJobDag records states, that is, it knows what task needs to be offered up to the Controller for scheduling. This saves the redundant computation for the Controller every time it goes through the TaskSchedulingStage of the Task pipeline.

![Architecture](./images/quota_InstanceCapacityManager.jpeg)

## User Manual

### How it works

Quota-based task scheduling works as follows. If a quota type is set, Task Framework will calculate a ratio against the sum of all quota config numbers for each quota type. Then it will apply that ratio to find the actual resource amount allotted to each quota type. Here is an example to illustrate this: Suppose the quota config is as follows:

```json
"QUOTA_TYPES":{
  "A":"2"
  ,"B":"1"
  ,"DEFAULT":"1"
}
```

Based on these raw numbers, Task Framework will compute the ratios. With the ratios, Task Framework will apply them to find the actual resource amount per quota type. The following table summarizes these calculations with  **the assumption of 40 JVM threads per instance**:

| Quota Type | Quota Config | Ratio | Actual Resource Allotted (# of JVM Threads) |
|:----------:|:------------:|:-----:|:-------------------------------------------:|
|      A     |       2      |  50%  |                      20                     |
|      B     |       1      |  25%  |                      10                     |
|   DEFAULT  |       1      |  25%  |                      10                     |


Every instance (node) will have a quota profile that looks like this. This has a few implications. First, this allows for  **prioritization of certain jobs by allotting a greater amount of resources to corresponding quota types**. In that sense, you may treat quota config numbers/ratios as user-defined priority values. More specifically, take the quota profile in the example above. In this case, when there are 100 jobs submitted for each quota type, jobs of type A will finish faster; in other words, quota type A will see twice as much throughput when there is a continuous stream of jobs due to its quota ratio being twice that of other quota types.

Quota-based task scheduling also allows for  **isolation/compartmentalization in scheduling jobs**. Suppose there are two categories of jobs, with the first category being  _urgent_  jobs that are short-lived but need to be run right away. On the other hand, suppose that the second category of jobs tend to take longer, but they aren't as urgent and can take their time running. Previously, these two types of jobs will get assigned, scheduled, and run in a mix, and it was indeed difficult to ensure that jobs in the first category be processed in an urgent manner. Quota-based scheduling solves this problem by allowing the user to create quota types that model "categories" with different characteristics and requirements.


### How to use

- Setting a Quota Config in ClusterConfig

In order to use quota-based task scheduling, you must establish a quota config first. This is a one-time operation, and once you verified that your ClusterConfig has a quota config set, there is no need to set it again. See the following code snippet for example:

```java
ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME); // Retrieve ClusterConfig
clusterConfig.resetTaskQuotaRatioMap(); // Optional: you may want to reset the quota config before creating a new quota config
clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 10); // Define the default quota (DEFAULT_QUOTA_TYPE = "DEFAULT")
clusterConfig.setTaskQuotaRatio("A", 20); // Define quota type A
clusterConfig.setTaskQuotaRatio("B", 10); // Define quota type B
_manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig); // Set the new ClusterConfig
```

A word of caution - if you do set the quota config, you  **must** **always define the default quota type (with the key "DEFAULT")**. Otherwise, jobs with no type information will no longer be scheduled and run. If you have been using Task Framework prior to the inception of quota-based scheduling, you might have recurrent workflows whose jobs do not have any type set. If you neglect to include the default quota type, these recurrent workflows will not execute properly.

Upon setting the quota config in ClusterConfig, you will see the updated field in your ZooKeeper cluster config ZNode in the JSON format. See an example below:

```json
{
  "id":"Example_Cluster"
  ,"simpleFields":{
    "allowParticipantAutoJoin":"true"
  }
  ,"listFields":{
  }
  ,"mapFields":{
    "QUOTA_TYPES":{
      "A":"20"
      ,"B":"10"
      ,"DEFAULT":"10"
    }
  }
}
```

- Setting a quota type for workflows and jobs
The Builders for WorkflowConfig and JobConfig provides a method for setting the quota type for the job. See below:
```java
JobConfig.Builder jobBuilderA =
    new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
        .addTaskConfigs(taskConfigsA).setNumConcurrentTasksPerInstance(50).setJobType("A"); // Setting the job quota type as "A"
workflowBuilder.addJob("JOB_A", jobBuilderA);
```

## FAQ
-   What happens if I don't set a quota config in ClusterConfig?
    -   When no quota config is found in ClusterConfig, Task Framework will treat all incoming jobs as DEFAULT and will give 100% of quota resources to the default type.
-   What happens if my job doesn't have a quota type set?
    -   If Task Framework encounters a job without a quota type (that is, either the quotaType field is missing, is an empty String, or a literal "null"), then the job will be treated as a DEFAULT job.
-   What if there is a workflow/job whose quota type does not exist in the quota config I have in ClusterConfig?
    -   Task Framework will  **not**  be able to locate the correct quota type, so it would  **treat it as the DEFAULT type**  and will assign and schedule accordingly using the quota for the DEFAULT type.
-   What about targeted jobs?
    -   Quotas will also apply to targeted jobs, each task of the targeted job taking up a pre-set resource amount (currently each task occupies 1 JVM thread).
-   What about job queues?
    -   Quota-based scheduling applies to all types of workflows - both generic workflows and job queues. A word of caution for the user is to be careful and always verify whether a job's quota type has been properly set. Task Framework will  **not**  automatically delete or inform the user of the jobs that are stuck due to an invalid quota type, so we caution all users to make sure the quota type exists by querying their settings in ClusterConfig.

## Future Steps

Quota-based task scheduling has been tested internally at LinkedIn and has been integrated into [Apache Gobblin](https://gobblin.apache.org/), enabling users of Helix Task Framework and Gobblin's Job Launcher to define categories and corresponding quota values. There are a few immediate to-do's that will improve the usability of this feature:

- More fine-grained quota profile

Currently, quota profiles apply across the entire cluster; that is, one quota profile defined in ClusterConfig will apply globally for all Participants. However, some use cases may require that each Participant have a different quota profile.

- Making Participants' maximum JVM thread capacity configurable

Helix Task Framework has the maximum number of task threads set at 40. Making this configurable will potentially allow some users to increase throughput of tasks depending on the duration of execution of such tasks.

- Adding more dimensions to quota resource type

Currently, the number of JVM threads per Participant is the only dimension where Helix Task Framework defines quota in. However, as discussed in earlier sections, this is extendable to commonly-used constraints such as CPU usage, memory usage, or disk usage. As new dimensions are added, there will need to be additional implementation of the TaskAssigner interface that produces assignments for tasks based on constraints.
