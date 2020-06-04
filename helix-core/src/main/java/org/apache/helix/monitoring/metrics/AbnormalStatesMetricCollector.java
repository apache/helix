package org.apache.helix.monitoring.metrics;

import javax.management.JMException;

import org.apache.helix.HelixException;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.metrics.implementation.RebalanceCounter;
import org.apache.helix.monitoring.metrics.model.CountMetric;

public class AbnormalStatesMetricCollector extends MetricCollector {
  private static final String ABNORMAL_STATES_ENTITY_NAME = "AbnormalStates";

  public enum AbnormalStatesMetricNames {
    AbnormalStatePartitionCounter, RecoveryAttemptCounter
  }

  public AbnormalStatesMetricCollector(String clusterName, String stateModelDef) {
    super(MonitorDomainNames.Rebalancer.name(), clusterName,
        String.format("%s.%s", ABNORMAL_STATES_ENTITY_NAME, stateModelDef));
    createMetrics();
    if (clusterName != null) {
      try {
        register();
      } catch (JMException e) {
        throw new HelixException("Failed to register MBean for the AbnormalStatesMetricCollector",
            e);
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
