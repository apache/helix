package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.common.SensorNameProvider;

public interface ThreadPoolExecutorMonitorMBean extends SensorNameProvider {
  int getThreadPoolCoreSizeGauge();
  int getThreadPoolMaxSizeGauge();
  int getNumOfActiveThreadsGauge();
  int getQueueSizeGauge();
}
