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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.helix.messaging.handling.BatchMessageWrapper;

public abstract class StateModelFactory<T extends StateModel> {
  /**
   * mapping resourceName to map of partitionName to StateModel
   */
  private final ConcurrentMap<String, ConcurrentMap<String, T>> _stateModelMap =
      new ConcurrentHashMap<String, ConcurrentMap<String, T>>();

  /**
   * mapping from resourceName to BatchMessageWrapper
   */
  private final ConcurrentMap<String, BatchMessageWrapper> _batchMsgWrapperMap =
      new ConcurrentHashMap<String, BatchMessageWrapper>();

  /**
   * This method will be invoked only once per resource per partition per session
   * Replacing old StateModelFactory#createNewStateModel(String partitionName)
   * Add "resourceName" to signature @see HELIX-552
   * @param resourceName
   * @param partitionName
   * @return state model
   */
  public abstract T createNewStateModel(String resourceName, String partitionName);

  /**
   * Create a state model for a partition
   * @param partitionKey
   */
  public T createAndAddStateModel(String resourceName, String partitionKey) {
    T stateModel = createNewStateModel(resourceName, partitionKey);
    synchronized(_stateModelMap) {
      if (!_stateModelMap.containsKey(resourceName)) {
        _stateModelMap.put(resourceName, new ConcurrentHashMap<String, T>());
      }
      _stateModelMap.get(resourceName).put(partitionKey, stateModel);
    }
    return stateModel;
  }

  /**
   * Get the state model for a partition
   * @param resourceName
   * @param partitionKey
   * @return state model if exists, null otherwise
   */
  public T getStateModel(String resourceName, String partitionKey) {
    Map<String, T> map = _stateModelMap.get(resourceName);
    return map == null? null : map.get(partitionKey);
  }

  /**
   * remove state model for a partition
   * @param resourceName
   * @param partitionKey
   * @return state model removed or null if not exist
   */
  public T removeStateModel(String resourceName, String partitionKey) {
    T stateModel = null;
    synchronized(_stateModelMap) {
      Map<String, T> map = _stateModelMap.get(resourceName);
      if (map != null) {
        stateModel = map.remove(partitionKey);
        if (map.isEmpty()) {
          _stateModelMap.remove(resourceName);
        }
      }
    }
    return stateModel;
  }

  /**
   * get resource set
   * @param resourceName
   * @return resource name set
   */
  public Set<String> getResourceSet() {
    return _stateModelMap.keySet();
  }

  /**
   * get partition set for a resource
   * @param resourceName
   * @return partition key set
   */
  public Set<String> getPartitionSet(String resourceName) {
    Map<String, T> map = _stateModelMap.get(resourceName);
    return (map == null? Collections.<String>emptySet() : map.keySet());
  }

  /**
   * create a default batch-message-wrapper for a resource
   * @param resourceName
   * @return
   */
  public BatchMessageWrapper createBatchMessageWrapper(String resourceName) {
    return new BatchMessageWrapper();
  }

  /**
   * create a batch-message-wrapper for a resource and put it into map
   * @param resourceName
   * @return
   */
  public BatchMessageWrapper createAndAddBatchMessageWrapper(String resourceName) {
    BatchMessageWrapper wrapper = createBatchMessageWrapper(resourceName);
    _batchMsgWrapperMap.put(resourceName, wrapper);
    return wrapper;
  }

  /**
   * get batch-message-wrapper for a resource
   * @param resourceName
   * @return
   */
  public BatchMessageWrapper getBatchMessageWrapper(String resourceName) {
    return _batchMsgWrapperMap.get(resourceName);
  }

}
