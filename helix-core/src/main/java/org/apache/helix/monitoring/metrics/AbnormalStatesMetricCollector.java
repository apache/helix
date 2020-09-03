package org.apache.helix.monitoring.metrics;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import javax.management.JMException;

import org.apache.helix.HelixException;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.metrics.implementation.RebalanceCounter;
import org.apache.helix.monitoring.metrics.model.CountMetric;

public class AbnormalStatesMetricCollector extends MetricCollector {
  private static final String ABNORMAL_STATES_ENTITY_NAME = "AbnormalStates";

  /**
   * This enum class contains all metric names defined for AbnormalStateResolver.
   * Note that all enums are in camel case for readability.
   */
  public enum AbnormalStatesMetricNames {
    // The counter of the partitions that contains abnormal state.
    AbnormalStatePartitionCounter,
    // The counter of the attempts that the resolver tries to recover the abnormal state.
    RecoveryAttemptCounter
  }

  public AbnormalStatesMetricCollector(String clusterName, String stateModelDef) {
    super(MonitorDomainNames.Rebalancer.name(), clusterName,
        String.format("%s.%s", ABNORMAL_STATES_ENTITY_NAME, stateModelDef));
    createMetrics();
    if (clusterName != null) {
      try {
        register();
      } catch (JMException e) {
        throw new HelixException(
            "Failed to register MBean for the " + AbnormalStatesMetricCollector.class
                .getSimpleName(), e);
      }
    }
  }

  private void createMetrics() {
    // Define all metrics
    CountMetric abnormalStateReplicasCounter =
        new RebalanceCounter(AbnormalStatesMetricNames.AbnormalStatePartitionCounter.name());
    CountMetric RecoveryAttemptCounter =
        new RebalanceCounter(AbnormalStatesMetricNames.RecoveryAttemptCounter.name());
    addMetric(abnormalStateReplicasCounter);
    addMetric(RecoveryAttemptCounter);
  }
}
