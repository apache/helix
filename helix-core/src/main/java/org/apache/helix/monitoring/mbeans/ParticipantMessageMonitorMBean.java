package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;


public interface ParticipantMessageMonitorMBean extends SensorNameProvider {
  public long getReceivedMessages();
}