package com.linkedin.clustermanager.monitoring;

import com.linkedin.clustermanager.monitoring.mbeans.StateTransitionStatMonitor;

public interface TransStatMonitorChangedListener
{
  void onTransStatMonitorAdded(StateTransitionStatMonitor newStateTransitionStatMonitor);
}
