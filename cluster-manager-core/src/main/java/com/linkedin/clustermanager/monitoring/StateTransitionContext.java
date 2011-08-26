package com.linkedin.clustermanager.monitoring;

public class StateTransitionContext
{
  private final String _stateUnitGroup;
  private final String _clusterName;
  private final String _instanceName;
  private final String _transition;
  
  public StateTransitionContext(
      String clusterName, 
      String instanceName,
      String stateUnitGroup, 
      String transition
      )
  {
    _clusterName = clusterName;
    _stateUnitGroup = stateUnitGroup; 
    _transition = transition;
    _instanceName = instanceName;
  }
  
  public String getClusterName()
  {
    return _clusterName;
  }
  
  public String getInstanceName()
  {
    return _instanceName;
  }
  
  public String getStateUnitGroup()
  {
    return _stateUnitGroup;
  }
  
  public String getTransition()
  {
    return _transition;
  }
  
  
  public boolean equals(StateTransitionContext other)
  {
    return 
      _clusterName.equals(other.getClusterName()) &&
      _instanceName.equals(other.getInstanceName()) &&
      _stateUnitGroup.equals(other.getStateUnitGroup()) &&
      _transition.equals(other.getTransition()) ;
  }
  
  public int hashCode()
  {
    return toString().hashCode();
  }
  
  public String toString()
  {
     return "type=test,Cluster=" + _clusterName + "," + 
           "instance=" + _instanceName + "," +
           "ResourceGroup=" + _stateUnitGroup +"," + 
           "Transition=" + _transition;    
  }
  
}
