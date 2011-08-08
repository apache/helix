package com.linkedin.clustermanager.controller.stages;

import java.util.Map;

import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.StateModelDefinition;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData
 * which provides useful methods to search/lookup properties
 * 
 * @author kgopalak
 * 
 */
public class ClusterDataCache
{
  private Map<String, LiveInstance> _liveInstanceMap;
  private Map<String, IdealState> _idealStateMap;
  private Map<String, StateModelDefinition> _stateModelDefMap;
  private Map<String, InstanceConfig> _instanceConfigMap;

  public boolean refresh()
  {
    return false;
  }

  public void setLiveInstanceMap(Map<String, LiveInstance> liveInstanceMap)
  {
    _liveInstanceMap = liveInstanceMap;

  }

  public void setIdealStateMap(Map<String, IdealState> idealStateMap)
  {
    _idealStateMap = idealStateMap;

  }

  public void setStateModelDefMap(
      Map<String, StateModelDefinition> stateModelDefMap)
  {
    _stateModelDefMap = stateModelDefMap;
  }

  public void setInstanceConfigMap(Map<String, InstanceConfig> instanceConfigMap)
  {
    _instanceConfigMap = instanceConfigMap;
    // TODO Auto-generated method stub

  }

}
