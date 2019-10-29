package org.apache.helix;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;

/**
 * CONTROLLER: cluster managing component is a controller
 * PARTICIPANT: participate in the cluster state changes
 * SPECTATOR: interested in the state changes in the cluster
 * CONTROLLER_PARTICIPANT:
 * special participant that competes for the leader of CONTROLLER_CLUSTER
 * used in cluster controller of distributed mode {@HelixControllerMain}
 */
public enum InstanceType {
  CONTROLLER(new String[] {
      MonitorDomainNames.ClusterStatus.name(),
      MonitorDomainNames.HelixZkClient.name(),
      MonitorDomainNames.HelixCallback.name(),
      MonitorDomainNames.Rebalancer.name()
  }),

  PARTICIPANT(new String[] {
      MonitorDomainNames.CLMParticipantReport.name(),
      MonitorDomainNames.HelixZkClient.name(),
      MonitorDomainNames.HelixCallback.name(),
      MonitorDomainNames.HelixThreadPoolExecutor.name()
  }),

  CONTROLLER_PARTICIPANT(new String[] {
      MonitorDomainNames.ClusterStatus.name(),
      MonitorDomainNames.HelixZkClient.name(),
      MonitorDomainNames.HelixCallback.name(),
      MonitorDomainNames.HelixThreadPoolExecutor.name(),
      MonitorDomainNames.CLMParticipantReport.name(),
      MonitorDomainNames.Rebalancer.name()
  }),

  SPECTATOR(new String[] {
      MonitorDomainNames.HelixZkClient.name()
  }),

  ADMINISTRATOR(new String[] {
      MonitorDomainNames.HelixZkClient.name()
  });

  private final String[] _monitorDomains;

  InstanceType(String[] monitorDomains) {
    _monitorDomains = monitorDomains;
  }

  public List<String> getActiveMBeanDomains() {
    return Arrays.asList(_monitorDomains);
  }
}
