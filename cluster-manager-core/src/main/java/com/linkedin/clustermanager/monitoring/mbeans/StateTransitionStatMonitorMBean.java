package com.linkedin.clustermanager.monitoring.mbeans;

public interface StateTransitionStatMonitorMBean
{
  @Description("Total state transitions happened")
  long getTotalStateTransitions();
  
  @Description("Total failed transitions ")
  long getTotalFailedTransitions();
  
  @Description("Total successful transitions")
  long getTotalSuccessTransitions();
  
  @Description("Mean transistion latency")
  double getMeanTransitionLatency();
  
  @Description("Max transistion latency")
  double getMaxTransitionLatency();
  
  @Description("Min transistion latency")
  double getMinTransitionLatency();

  @Description("Transistion latency at X top percentage")
  double getPercentileTransitionLatency(int percentage);
  
  @Description("")
  void reset();
}
