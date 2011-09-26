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
  
  @Override
  public boolean equals(Object other)
  {
    if(! (other instanceof StateTransitionContext))
    {
      return false;
    }
    
    StateTransitionContext otherCxt = (StateTransitionContext) other;  
    return
      _clusterName.equals(otherCxt.getClusterName()) &&
      // _instanceName.equals(otherCxt.getInstanceName()) &&
      _stateUnitGroup.equals(otherCxt.getStateUnitGroup()) &&
      _transition.equals(otherCxt.getTransition()) ;
  }
    

  // In the report, we will gather per transition time statistics
 @Override
  public int hashCode()
  {
    return toString().hashCode();
  }
  
  public String toString()
  {
     return "Cluster=" + _clusterName + "," + 
           // "instance=" + _instanceName + "," +
           "ResourceGroup=" + _stateUnitGroup +"," + 
           "Transition=" + _transition;    
  }
  
}
