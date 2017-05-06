package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;

public interface HelixCallbackMonitorMBean extends SensorNameProvider {
  long getCallbackCounter();
  long getCallbackUnbatchedCounter();
  long getCallbackLatencyCounter();

  long getIdealStateCallbackCounter();
  long getIdealStateCallbackUnbatchedCounter();
  long getIdealStateCallbackLatencyCounter();

  long getInstanceConfigCallbackCounter();
  long getInstanceConfigCallbackUnbatchedCounter();
  long getInstanceConfigCallbackLatencyCounter();

  long getConfigCallbackCounter();
  long getConfigCallbackUnbatchedCounter();
  long getConfigCallbackLatencyCounter();

  long getLiveInstanceCallbackCounter();
  long getLiveInstanceCallbackUnbatchedCounter();
  long getLiveInstanceCallbackLatencyCounter();

  long getCurrentStateCallbackCounter();
  long getCurrentStateCallbackUnbatchedCounter();
  long getCurrentStateCallbackLatencyCounter();

  long getMessageCallbackCounter();
  long getMessageCallbackUnbatchedCounter();
  long getMessageCallbackLatencyCounter();

  long getMessagesControllerCallbackCounter();
  long getMessagesControllerCallbackUnbatchedCounter();
  long getMessagesControllerCallbackLatencyCounter();

  long getExternalViewCallbackCounter();
  long getExternalViewCallbackUnbatchedCounter();
  long getExternalViewCallbackLatencyCounter();

  long getControllerCallbackCounter();
  long getControllerCallbackUnbatchedCounter();
  long getControllerCallbackLatencyCounter();
}
