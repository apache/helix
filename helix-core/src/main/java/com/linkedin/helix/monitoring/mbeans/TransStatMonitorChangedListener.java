package com.linkedin.helix.monitoring.mbeans;


public interface TransStatMonitorChangedListener
{
  void onTransStatMonitorAdded(StateTransitionStatMonitor newStateTransitionStatMonitor);
}
