package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;

public interface ControllerStageMonitorMBean extends SensorNameProvider {
  long getStageProcessedCounter();
  long getStageProcessedLatencyCounter();
}
