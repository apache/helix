package org.apache.helix.monitoring.mbeans;

/**
 * Indicate how many metrics should be emitted.
 *
 * TODO This config only applies to HelixManager's ZkClient related metrics only for now. Shall apply monitor level to the other metrics if necessary.
 */
public enum MonitorLevel {
  DEFAULT, // Emitting the default metrics
  ALL, // Emitting all possible metrics
  AGGREGATED_ONLY // Emitting only the aggregated metrics if applicable.
}
