package org.apache.helix.participant.statemachine;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.helix.messaging.handling.BatchMessageWrapper;

public abstract class StateModelFactory<T extends StateModel>
{
  // partitionName -> StateModel
  private ConcurrentMap<String, T> _stateModelMap = new ConcurrentHashMap<String, T>();
  
  // resourceName -> BatchMessageWrapper
  private final ConcurrentMap<String, BatchMessageWrapper> _batchMsgWrapperMap
      = new ConcurrentHashMap<String, BatchMessageWrapper>();

  /**
   * This method will be invoked only once per partitionName per session
   * 
   * @param partitionName
   * @return
   */
  public abstract T createNewStateModel(String partitionName);

//  /**
//   * Add a state model for a partition
//   * 
//   * @param partitionName
//   * @return
//   */
//  public void addStateModel(String partitionName, T stateModel)
//  {
//    _stateModelMap.put(partitionName, stateModel);
//  }
  
  /**
   * Create a state model for a partition
   * 
   * @param partitionName
   */
  public T createAndAddStateModel(String partitionName)
  {
		T stateModel = createNewStateModel(partitionName);
	    _stateModelMap.put(partitionName, stateModel);
	    return stateModel;
  }

  /**
   * Get the state model for a partition
   * 
   * @param partitionName
   * @return
   */
  public T getStateModel(String partitionName)
  {
    return _stateModelMap.get(partitionName);
  }

  /**
   * Get the state model map
   * 
   * @return
   */
  public Map<String, T> getStateModelMap()
  {
    return _stateModelMap;
  }
  
  /**
   * create a default batch-message-wrapper for a resource
   * 
   * @param resourceName
   * @return
   */
  public BatchMessageWrapper createBatchMessageWrapper(String resourceName)
  {
    return new BatchMessageWrapper();
  }
  
  /**
   * create a batch-message-wrapper for a resource and put it into map
   * 
   * @param resourceName
   * @return
   */
  public BatchMessageWrapper createAndAddBatchMessageWrapper(String resourceName)
  {
    BatchMessageWrapper wrapper = createBatchMessageWrapper(resourceName);
    _batchMsgWrapperMap.put(resourceName, wrapper);
    return wrapper;
  }
  
  /**
   * get batch-message-wrapper for a resource
   * 
   * @param resourceName
   * @return
   */
  public BatchMessageWrapper getBatchMessageWrapper(String resourceName)
  {
    return _batchMsgWrapperMap.get(resourceName);
  }
  
}
