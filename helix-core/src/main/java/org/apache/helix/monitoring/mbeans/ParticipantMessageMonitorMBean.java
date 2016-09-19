package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;


public interface ParticipantMessageMonitorMBean extends SensorNameProvider {
  public long getReceivedMessages();
  public long getDiscardedMessages();
  public long getCompletedMessages();
  public long getFailedMessages();
  public long getPendingMessages();
}