package com.linkedin.helix.monitoring.mbeans;


public interface StateTransitionStatMonitorMBean
{
  long getTotalStateTransitionGauge();
  
  long getTotalFailedTransitionGauge();
  
  long getTotalSuccessTransitionGauge();
  
  double getMeanTransitionLatency();
  
  double getMaxTransitionLatency();
  
  double getMinTransitionLatency();

  double getPercentileTransitionLatency(int percentage);
  
  double getMeanTransitionExecuteLatency();
  
  double getMaxTransitionExecuteLatency();
  
  double getMinTransitionExecuteLatency();

  double getPercentileTransitionExecuteLatency(int percentage);
  
  void reset();
}
