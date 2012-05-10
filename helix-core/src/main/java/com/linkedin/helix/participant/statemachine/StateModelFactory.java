/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
