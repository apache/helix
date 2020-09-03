package org.apache.helix.controller.rebalancer.constraint;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.api.rebalancer.constraint.AbnormalStateResolver;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.metrics.AbnormalStatesMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;

/**
 * A wrap class to add monitor functionality into an AbnormalStateResolver implementation.
 */
public class MonitoredAbnormalResolver implements AbnormalStateResolver {
  private final AbnormalStateResolver _resolver;
  private final AbnormalStatesMetricCollector _metricCollector;

  /**
   * A placeholder which will be used when the resolver is not specified.
   * This is a dummy class that does not really functional.
   */
  public final static MonitoredAbnormalResolver DUMMY_STATE_RESOLVER =
      new MonitoredAbnormalResolver(new AbnormalStateResolver() {
        public boolean checkCurrentStates(final CurrentStateOutput currentStateOutput,
            final String resourceName, final Partition partition,
            final StateModelDefinition stateModelDef) {
          // By default, all current states are valid.
          return true;
        }

        public Map<String, String> computeRecoveryAssignment(
            final CurrentStateOutput currentStateOutput, final String resourceName,
            final Partition partition, final StateModelDefinition stateModelDef,
            final List<String> preferenceList) {
          throw new UnsupportedOperationException("This resolver won't recover abnormal states.");
        }
      }, null);

  private MonitoredAbnormalResolver(AbnormalStateResolver resolver,
      AbnormalStatesMetricCollector metricCollector) {
    if (resolver instanceof MonitoredAbnormalResolver) {
      throw new IllegalArgumentException(
          "Cannot construct a MonitoredAbnormalResolver wrap object using another MonitoredAbnormalResolver object.");
    }
    _resolver = resolver;
    _metricCollector = metricCollector;
  }

  public MonitoredAbnormalResolver(AbnormalStateResolver resolver, String clusterName,
      String stateModelDef) {
    this(resolver, new AbnormalStatesMetricCollector(clusterName, stateModelDef));
  }

  public void recordAbnormalState() {
    _metricCollector.getMetric(
        AbnormalStatesMetricCollector.AbnormalStatesMetricNames.AbnormalStatePartitionCounter.name(),
        CountMetric.class).increment(1);
  }

  public void recordRecoveryAttempt() {
    _metricCollector.getMetric(
        AbnormalStatesMetricCollector.AbnormalStatesMetricNames.RecoveryAttemptCounter.name(),
        CountMetric.class).increment(1);
  }

  public Class getResolverClass() {
    return _resolver.getClass();
  }

  @Override
  public boolean checkCurrentStates(CurrentStateOutput currentStateOutput, String resourceName,
      Partition partition, StateModelDefinition stateModelDef) {
    return _resolver
        .checkCurrentStates(currentStateOutput, resourceName, partition, stateModelDef);
  }

  @Override
  public Map<String, String> computeRecoveryAssignment(CurrentStateOutput currentStateOutput,
      String resourceName, Partition partition, StateModelDefinition stateModelDef,
      List<String> preferenceList) {
    return _resolver
        .computeRecoveryAssignment(currentStateOutput, resourceName, partition, stateModelDef,
            preferenceList);
  }

  public void close() {
    if (_metricCollector != null) {
      _metricCollector.unregister();
    }
  }

  @Override
  public void finalize() {
    close();
  }
}
