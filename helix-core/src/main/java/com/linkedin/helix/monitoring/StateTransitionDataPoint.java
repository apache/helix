package com.linkedin.helix.monitoring;

public class StateTransitionDataPoint
{
  long _totalDelay;
  long _executionDelay;
  boolean _isSuccess;
  
  public StateTransitionDataPoint(long totalDelay, long executionDelay, boolean isSuccess)
  {
    _totalDelay = totalDelay;
    _executionDelay = executionDelay;
    _isSuccess = isSuccess;
  }
  
  public long getTotalDelay()
  {
    return _totalDelay;
  }
  
  public long getExecutionDelay()
  {
    return _executionDelay;
  }
  
  public boolean getSuccess()
  {
    return _isSuccess;
  }
}
