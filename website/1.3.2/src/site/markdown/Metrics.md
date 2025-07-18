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

Helix Monitoring Metrics
------------------

Helix monitoring metrics are exposed as the MBeans attributes.
The MBeans are registered based on instance role.

The easiest way to see the available metrics is using jconsole and point it at a running Helix instance.
This will allow browsing all metrics with JMX.

Note that if not mentioned in the attribute name, all attributes are gauge by default.

### Metrics on Both Controller and Participant

#### MBean ZkClientMonitor
ObjectName: "HelixZkClient:type=[client-type],key=[specified-client-key],PATH=[zk-client-listening-path]"

|Attributes|Description|
|----------|-----------|
|ReadCounter|Zk Read counter. Which could be used to identify unusually high/low ZK traffic|
|WriteCounter|Same as above|
|ReadBytesCounter|Same as above|
|WriteBytesCounter|Same as above|
|StateChangeEventCounter|Zk connection state change counter. Which could be used to identify ZkClient unstable connection|
|DataChangeEventCounter|Zk node data change counter. which could be used to identify unusual high/low ZK events occurrence or slow event processing|
|PendingCallbackGauge|Number of the pending Zk callbacks.|
|TotalCallbackCounter|Number of total received Zk callbacks.|
|TotalCallbackHandledCounter|Number of total handled Zk callbacks.|
|ReadTotalLatencyCounter|Total read latency in ms.|
|WriteTotalLatencyCounter|Total write latency in ms.|
|WriteFailureCounter|Total write failures.|
|ReadFailureCounter|Total read failures.|
|ReadLatencyGauge|Histogram (with all statistic data) of read latency.|
|WriteLatencyGauge|Histogram (with all statistic data) of write latency.|
|ReadBytesGauge|Histogram (with all statistic data) of read bytes of single Zk access.|
|WriteBytesGauge|Histogram (with all statistic data) of write bytes of single Zk access.|

#### MBean HelixCallbackMonitor
ObjectName: "HelixCallback:Type=[callback-type],Key=[cluster-name].[instance-name],Change=[callback-change-type]"

|Attributes|Description|
|----------|-----------|
|Counter|Zk Callback counter for each Helix callback type.|
|UnbatchedCounter|Unbatched Zk Callback counter for each helix callback type.|
|LatencyCounter|Callback handler latency counter in ms.|
|LatencyGauge|Histogram (with all statistic data) of Callback handler latency.|

#### MBean MessageQueueMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],messageQueue=[instance-name]"

|Attributes|Description|
|----------|-----------|
|MessageQueueBacklog|Get the message queue size|

### Metrics on Controller only

#### MBean ClusterStatusMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name]"

|Attributes|Description|
|----------|-----------|
|DisabledInstancesGauge|Current  number of disabled instances|
|DisabledPartitionsGauge|Current number of disabled partitions number|
|DownInstanceGauge|Current down instances number|
|InstanceMessageQueueBacklog|The sum of all message queue sizes for instances in this cluster|
|InstancesGauge|Current live instances number|
|MaxMessageQueueSizeGauge|The maximum message queue size across all instances including controller|
|RebalanceFailureGauge|None 0 if previous rebalance failed unexpectedly. The Gauge will be set every time rebalance is done.|
|RebalanceFailureCounter|The number of failures during rebalance pipeline.|
|Enabled|1 if cluster is enabled, otherwise 0|
|Maintenance|1 if cluster is in maintenance mode, otherwise 0|
|Paused|1 if cluster is paused, otherwise 0|

#### MBean ClusterEventMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],eventName=ClusterEvent,phaseName=[event-handling-phase]"

|Attributes|Description|
|----------|-----------|
|TotalDurationCounter|Total event process duration for each stage.|
|MaxSingleDurationGauge|Max event process duration for each stage within the recent hour.|
|EventCounter|The count of processed event in each stage.|
|DurationGauge|Histogram (with all statistic data) of event process duration for each stage.|

#### MBean InstanceMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],instanceName=[instance-name]"

|Attributes|Description|
|----------|-----------|
|Online|This instance is Online (1) or Offline (0)|
|Enabled|This instance is Enabled (1) or Disabled (0)|
|TotalMessageReceived|Number of messages sent to this instance by controller|
|DisabledPartitions|Get the total disabled partitions number for this instance|

#### MBean ResourceMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],resourceName=[resource-name]"

|Attributes|Description|
|----------|-----------|
|PartitionGauge|Get number of partitions of the resource in best possible ideal state for this resource|
|ErrorPartitionGauge|Get the number of current partitions in ERORR state for this resource|
|DifferenceWithIdealStateGauge|Get the number of how many replicas' current state are different from ideal state for this resource|
|MissingTopStatePartitionGauge|Get the number of partitions do not have top state for this resource|
|ExternalViewPartitionGauge|Get number of partitions in ExternalView for this resource|
|TotalMessageReceived|Get number of messages sent to this resource by controller|
|LoadRebalanceThrottledPartitionGauge|Get number of partitions that need load rebalance but were throttled.|
|RecoveryRebalanceThrottledPartitionGauge|Get number of partitions that need recovery rebalance but were throttled.|
|PendingLoadRebalancePartitionGauge|Get number of partitions that have pending load rebalance requests.|
|PendingRecoveryRebalancePartitionGauge|Get number of partitions that have pending recovery rebalance requests.|
|MissingReplicaPartitionGauge|Get number of partitions that have replica number smaller than expected.|
|MissingMinActiveReplicaPartitionGauge|Get number of partitions that have replica number smaller than the minimum requirement.|
|MaxSinglePartitionTopStateHandoffDurationGauge|Get the max duration recorded when the top state is missing in any single partition.|
|FailedTopStateHandoffCounter|	Get the number of total top state transition failure.|
|SucceededTopStateHandoffCounter|Get the number of total top state transition done successfully.|
|SuccessfulTopStateHandoffDurationCounter|Get the total duration of all top state transitions.|
|PartitionTopStateHandoffDurationGauge|Histogram (with all statistic data) of top state transition duration.|

#### MBean PerInstanceResourceMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],instanceName=[instance-name],resourceName=[resource-name]"

|Attributes|Description|
|----------|-----------|
|PartitionGauge|Get number of partitions of the resource in best possible ideal state for this resource on specific instance|

#### MBean JobMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],jobType=[job-type]"

|Attributes|Description|
|----------|-----------|
|SuccessfulJobCount|Get number of the succeeded jobs|
|FailedJobCount|Get number of failed jobs|
|AbortedJobCount|Get number of the aborted jobs|
|ExistingJobGauge|Get number of existing jobs registered|
|QueuedJobGauge|Get numbers of queued jobs, which are not running jobs|
|RunningJobGauge|Get numbers of running jobs|
|MaximumJobLatencyGauge|Get maximum latency of jobs running time. It will be cleared every hour|
|JobLatencyCount|Get total job latency counter.|

#### MBean WorkflowMonitor
ObjectName: "ClusterStatus:cluster=[cluster-name],workflowType=[workflow-type]"

|Attributes|Description|
|----------|-----------|
|SuccessfulWorkflowCount|Get number of succeeded workflows|
|FailedWorkflowCount|Get number of failed workflows|
|FailedWorkflowGauge|Get number of current failed workflows|
|ExistingWorkflowGauge|Get number of current existing workflows|
|QueuedWorkflowGauge|Get number of queued but not started workflows|
|RunningWorkflowGauge|Get number of running workflows|
|WorkflowLatencyCount|Get workflow latency count|
|MaximumWorkflowLatencyGauge|Get maximum workflow latency gauge. It will be reset in 1 hour.|

### Metrics on Participant only

#### MBean StateTransitionStatMonitor
ObjectName: "CLMParticipantReport:Cluster=[cluster-name],Resource=[resource-name],Transition=[transaction-id]"

|Attributes|Description|
|----------|-----------|
|TotalStateTransitionGauge|Get the number of total state transitions|
|TotalFailedTransitionGauge|Get the number of total failed state transitions|
|TotalSuccessTransitionGauge|Get the number of total succeeded state transitions|
|MeanTransitionLatency|Get the average state transition latency (from message read to finish)|
|MaxTransitionLatency|Get the maximum state transition latency|
|MinTransitionLatency|Get the minimum state transition latency|
|PercentileTransitionLatency|Get the percentile of state transitions latency|
|MeanTransitionExecuteLatency|Get the average execution latency of state transition (from task started to finish)|
|MaxTransitionExecuteLatency|Get the maximum execution latency of state transition|
|MinTransitionExecuteLatency|Get the minimum execution latency of state transition|
|PercentileTransitionExecuteLatency|Get the percentile of execution latency of state transitions|

#### MBean ThreadPoolExecutorMonitor
ObjectName: "HelixThreadPoolExecutor:Type=[threadpool-type]" (threadpool-type in Message.MessageType, BatchMessageExecutor, Task)

|Attributes|Description|
|----------|-----------|
|ThreadPoolCoreSizeGauge|Thread pool size is as configured. Aggregate total thread pool size for the whole cluster.|
|ThreadPoolMaxSizeGauge|Same as above|
|NumOfActiveThreadsGauge|Number of running threads.|
|QueueSizeGauge|Queue size. Could be used to identify if too many HelixTask blocked in participant.|

#### MBean MessageLatencyMonitor
ObjectName: "CLMParticipantReport:ParticipantName=[instance-name],MonitorType=MessageLatencyMonitor"

|Attributes|Description|
|----------|-----------|
|TotalMessageCount|Total message count|
|TotalMessageLatency|Total message latency in ms|
|MessagelatencyGauge|Histogram (with all statistic data) of message processing latency.|

#### MBean ParticipantMessageMonitor
ObjectName: "CLMParticipantReport:ParticipantName=[instance-name]"

|Attributes|Description|
|----------|-----------|
|ReceivedMessages|Number of received messages|
|DiscardedMessages|Number of discarded messages|
|CompletedMessages|Number of completed messages|
|FailedMessages|Number of failed messages|
|PendingMessages|Number of pending messages to be processed|
