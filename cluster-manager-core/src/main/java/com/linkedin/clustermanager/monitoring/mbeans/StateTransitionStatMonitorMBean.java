package com.linkedin.clustermanager.monitoring.mbeans;

public interface StateTransitionStatMonitorMBean
{
  @Description("Total state transitions happened")
  long getTotalStateTransitionCount();
  
  @Description("Total failed transitions ")
  long getTotalFailedTransitionCount();
  
  @Description("Total successful transitions")
  long getTotalSuccessTransitionCount();
  
  @Description("Mean transition latency")
  double getMeanTransitionLatency();
  
  @Description("Max transition latency")
  double getMaxTransitionLatency();
  
  @Description("Min transition latency")
  double getMinTransitionLatency();

  @Description("Transition latency at X top percentage")
  double getPercentileTransitionLatency(int percentage);
  
  @Description("Mean transition execute latency")
  double getMeanTransitionExecuteLatency();
  
  @Description("Max transition execute latency")
  double getMaxTransitionExecuteLatency();
  
  @Description("Min transition execute latency")
  double getMinTransitionExecuteLatency();

  @Description("Transition execute latency at X top percentage")
  double getPercentileTransitionExecuteLatency(int percentage);
  
  @Description("Reset counters")
  void reset();
}
