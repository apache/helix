package com.linkedin.helix.participant.statemachine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class StateModelFactory<T extends StateModel>
{
  private ConcurrentMap<String, T> _stateModelMap = new ConcurrentHashMap<String, T>();

  /**
   * this method will be invoked only once per stateUnit key per session
   * 
   * @param stateUnitKey
   * @return
   */
  public abstract T createNewStateModel(String stateUnitKey);

  /**
   * @param stateUnitKey
   * @return
   */
  public void addStateModel(String stateUnitKey, T stateModel)
  {
    _stateModelMap.put(stateUnitKey, stateModel);
  }
  
  public void createAndAddStateModel(String stateUnitKey)
  {
    _stateModelMap.put(stateUnitKey, createNewStateModel(stateUnitKey));
  }

  public T getStateModel(String stateUnitKey)
  {
    return _stateModelMap.get(stateUnitKey);
  }

  public Map<String, T> getStateModelMap()
  {
    return _stateModelMap;
  }
}
