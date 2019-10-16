package org.apache.helix.monitoring.metrics.implementation;

import org.apache.helix.monitoring.metrics.model.CountMetric;

public class RebalanceFailureCount extends CountMetric {
  /**
   * Instantiates a new Simple dynamic metric.
   *
   * @param metricName the metric name
   */
  public RebalanceFailureCount(String metricName) {
    super(metricName, 0l);
  }

  public void increaseCount(long count) {
    updateValue(getValue() + count);
  }
}
