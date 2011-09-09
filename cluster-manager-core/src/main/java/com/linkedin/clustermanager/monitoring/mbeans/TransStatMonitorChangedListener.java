package com.linkedin.clustermanager.monitoring.mbeans;


public interface TransStatMonitorChangedListener
{
  void onTransStatMonitorAdded(StateTransitionStatMonitor newStateTransitionStatMonitor);
}
