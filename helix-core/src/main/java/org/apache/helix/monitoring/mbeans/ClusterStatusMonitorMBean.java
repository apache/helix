package org.apache.helix.monitoring.mbeans;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.monitoring.SensorNameProvider;

public interface ClusterStatusMonitorMBean extends SensorNameProvider {

  /**
   * @return number of instances that are down (non-live instances)
   */
  long getDownInstanceGauge();

  /**
   * @return total number of instances
   */
  long getInstancesGauge();

  /**
   * @return number of disabled instances
   */
  long getDisabledInstancesGauge();

  /**
   * @return number of disabled partitions
   */
  long getDisabledPartitionsGauge();

  /**
   * @return 1 if rebalance failed; 0 if rebalance did not fail
   */
  long getRebalanceFailureGauge();

  /**
   * The max message queue size across all instances including controller
   * @return
   */
  long getMaxMessageQueueSizeGauge();

  /**
   * The sum of all message queue sizes for instances in this cluster
   * @return
   */
  long getInstanceMessageQueueBacklog();

  /**
   * @return 1 if cluster is enabled, otherwise 0
   */
  long getEnabled();

  /**
   * @return 1 if cluster is in maintenance mode, otherwise 0
   */
  long getMaintenance();

  /**
   * @return 1 if cluster is paused, otherwise 0
   */
  long getPaused();

  /**
   * The number of failures during rebalance pipeline.
   * @return
   */
  long getRebalanceFailureCounter();

  /**
   * @return number of all resources in this cluster
   */
  long getTotalResourceGauge();

  /**
   * @return number of all partitions in this cluster
   */
  long getTotalPartitionGauge();

  /**
   * @return number of all partitions in this cluster that have errors
   */
  long getErrorPartitionGauge();

  /**
   * @return number of all partitions in this cluster without any top-state replicas
   */
  long getMissingTopStatePartitionGauge();

  /**
   * @return number of all partitions in this cluster without enough active replica
   */
  long getMissingMinActiveReplicaPartitionGauge();

  /**
   * @return number of all partitions in this cluster whose ExternalView and IdealState have discrepancies
   */
  long getDifferenceWithIdealStateGauge();

  /**
   * @return number of sent state transition messages in this cluster
   */
  long getStateTransitionCounter();

  /**
   * @return number of pending state transitions in this cluster
   */
  long getPendingStateTransitionGuage();
}
