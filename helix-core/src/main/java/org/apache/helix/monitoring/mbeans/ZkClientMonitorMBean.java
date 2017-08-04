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

public interface ZkClientMonitorMBean extends SensorNameProvider {
  long getStateChangeEventCounter();
  long getDataChangeEventCounter();

  long getReadCounter();
  long getWriteCounter();
  long getReadBytesCounter();
  long getWriteBytesCounter();
  long getTotalReadLatencyCounter();
  long getTotalWriteLatencyCounter();
  long getMaxSingleReadLatencyGauge();
  long getMaxSingleWriteLatencyGauge();
  long getReadFailureCounter();
  long getWriteFailureCounter();

  long getIdealStatesReadCounter();
  long getIdealStatesWriteCounter();
  long getIdealStatesReadBytesCounter();
  long getIdealStatesWriteBytesCounter();
  long getIdealStatesTotalReadLatencyCounter();
  long getIdealStatesTotalWriteLatencyCounter();
  long getIdealStatesMaxSingleReadLatencyGauge();
  long getIdealStatesMaxSingleWriteLatencyGauge();
  long getIdealStatesReadFailureCounter();
  long getIdealStatesWriteFailureCounter();

  long getInstancesReadCounter();
  long getInstancesWriteCounter();
  long getInstancesReadBytesCounter();
  long getInstancesWriteBytesCounter();
  long getInstancesTotalReadLatencyCounter();
  long getInstancesTotalWriteLatencyCounter();
  long getInstancesMaxSingleReadLatencyGauge();
  long getInstancesMaxSingleWriteLatencyGauge();
  long getInstancesReadFailureCounter();
  long getInstancesWriteFailureCounter();

  long getConfigsReadCounter();
  long getConfigsWriteCounter();
  long getConfigsReadBytesCounter();
  long getConfigsWriteBytesCounter();
  long getConfigsTotalReadLatencyCounter();
  long getConfigsTotalWriteLatencyCounter();
  long getConfigsMaxSingleReadLatencyGauge();
  long getConfigsMaxSingleWriteLatencyGauge();
  long getConfigsReadFailureCounter();
  long getConfigsWriteFailureCounter();

  long getControllerReadCounter();
  long getControllerWriteCounter();
  long getControllerReadBytesCounter();
  long getControllerWriteBytesCounter();
  long getControllerTotalReadLatencyCounter();
  long getControllerTotalWriteLatencyCounter();
  long getControllerMaxSingleReadLatencyGauge();
  long getControllerMaxSingleWriteLatencyGauge();
  long getControllerReadFailureCounter();
  long getControllerWriteFailureCounter();

  long getExternalViewReadCounter();
  long getExternalViewWriteCounter();
  long getExternalViewReadBytesCounter();
  long getExternalViewWriteBytesCounter();
  long getExternalViewTotalReadLatencyCounter();
  long getExternalViewTotalWriteLatencyCounter();
  long getExternalViewMaxSingleReadLatencyGauge();
  long getExternalViewMaxSingleWriteLatencyGauge();
  long getExternalViewReadFailureCounter();
  long getExternalViewWriteFailureCounter();

  long getLiveInstancesReadCounter();
  long getLiveInstancesWriteCounter();
  long getLiveInstancesReadBytesCounter();
  long getLiveInstancesWriteBytesCounter();
  long getLiveInstancesTotalReadLatencyCounter();
  long getLiveInstancesTotalWriteLatencyCounter();
  long getLiveInstancesMaxSingleReadLatencyGauge();
  long getLiveInstancesMaxSingleWriteLatencyGauge();
  long getLiveInstancesReadFailureCounter();
  long getLiveInstancesWriteFailureCounter();

  long getPropertyStoreReadCounter();
  long getPropertyStoreWriteCounter();
  long getPropertyStoreReadBytesCounter();
  long getPropertyStoreWriteBytesCounter();
  long getPropertyStoreTotalReadLatencyCounter();
  long getPropertyStoreTotalWriteLatencyCounter();
  long getPropertyStoreMaxSingleReadLatencyGauge();
  long getPropertyStoreMaxSingleWriteLatencyGauge();
  long getPropertyStoreReadFailureCounter();
  long getPropertyStoreWriteFailureCounter();

  long getCurrentStatesReadCounter();
  long getCurrentStatesWriteCounter();
  long getCurrentStatesReadBytesCounter();
  long getCurrentStatesWriteBytesCounter();
  long getCurrentStatesTotalReadLatencyCounter();
  long getCurrentStatesTotalWriteLatencyCounter();
  long getCurrentStatesMaxSingleReadLatencyGauge();
  long getCurrentStatesMaxSingleWriteLatencyGauge();
  long getCurrentStatesReadFailureCounter();
  long getCurrentStatesWriteFailureCounter();

  long getMessagesReadCounter();
  long getMessagesWriteCounter();
  long getMessagesReadBytesCounter();
  long getMessagesWriteBytesCounter();
  long getMessagesTotalReadLatencyCounter();
  long getMessagesTotalWriteLatencyCounter();
  long getMessagesMaxSingleReadLatencyGauge();
  long getMessagesMaxSingleWriteLatencyGauge();
  long getMessagesReadFailureCounter();
  long getMessagesWriteFailureCounter();
}
