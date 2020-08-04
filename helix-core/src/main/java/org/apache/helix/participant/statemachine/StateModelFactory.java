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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

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
   * Replace deprecating StateModelFactory#createNewStateModel(String partitionName)
   * Add "resourceName" to signature @see HELIX-552
   * @param resourceName
   * @param partitionName
   * @return state model
   */
  public T createNewStateModel(String resourceName, String partitionName) {
    // default implementation ignores resourceName
    return createNewStateModel(partitionName);
  }

  /**
   * NOTE: This method is deprecated. Bring it back to keep backward compatible.
   * Replaced by StateModelFactory#createNewStateModel(String resourceName, String partitionName)
   * This method will be invoked only once per partitionName per session
   * @param partitionName
   * @return state model
   */
  @Deprecated
  public T createNewStateModel(String partitionName) {
    throw new UnsupportedOperationException(
        "Please implement StateModelFactory#createNewStateModel(String resourceName, String partitionName)");
  }

  /**
   * Create a state model for a partition
   * @param partitionKey
   * @return state model
   */
  public T createAndAddStateModel(String resourceName, String partitionKey) {
    T stateModel = createNewStateModel(resourceName, partitionKey);
    synchronized (_stateModelMap) {
      if (!_stateModelMap.containsKey(resourceName)) {
        _stateModelMap.put(resourceName, new ConcurrentHashMap<String, T>());
      }
      _stateModelMap.get(resourceName).put(partitionKey, stateModel);
    }
    return stateModel;
  }

  /**
   * NOTE: This method is deprecated. Bring it back to keep backward compatible.
   * Replaced by StateModelFactory#createAndAddStateModel(String resourceName, String partitionKey)
   * Create a state model for a partition
   * @param partitionName
   * @return state model
   */
  @Deprecated
  public T createAndAddStateModel(String partitionName) {
    throw new UnsupportedOperationException(
        "This method is replaced by StateModelFactory#createAndAddStateModel(String resourceName, String partitionKey)");
  }

  /**
   * Get the state model for a partition
   * @param resourceName
   * @param partitionKey
   * @return state model if exists, null otherwise
   */
  public T getStateModel(String resourceName, String partitionKey) {
    Map<String, T> map = _stateModelMap.get(resourceName);
    return map == null ? null : map.get(partitionKey);
  }

  /**
   * NOTE: This method is deprecated. Bring it back to keep backward compatible.
   * Replaced by StateModelFactory#getStateModel(String resourceName, String partitionKey)
   * Get the state model for a partition
   * @param partitionName
   * @return state model if exists, null otherwise
   */
  @Deprecated
  public T getStateModel(String partitionName) {
    // return the first state model that match partitionName
    // assuming partitionName is unique across all resources
    for (ConcurrentMap<String, T> map : _stateModelMap.values()) {
      if (map.containsKey(partitionName)) {
        return map.get(partitionName);
      }
    }
    return null;
  }

  /**
   * remove state model for a partition
   * @param resourceName
   * @param partitionKey
   * @return state model removed or null if not exist
   */
  public T removeStateModel(String resourceName, String partitionKey) {
    T stateModel = null;
    synchronized (_stateModelMap) {
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
   * NOTE: This method is deprecated. Bring it back to keep backward compatible.
   * Replaced by StateModelFactory#removeStateModel(String resourceName, String partitionKey)
   * remove state model for a partition
   * @param partitionName
   * @return state model removed or null if not exist
   */
  @Deprecated
  public T removeStateModel(String partitionName) {
    // remove the first state model that match partitionName
    // assuming partitionName is unique across all resources
    for (ConcurrentMap<String, T> map : _stateModelMap.values()) {
      if (map.containsKey(partitionName)) {
        return map.remove(partitionName);
      }
    }
    return null;
  }

  /**
   * get resource set
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
    return (map == null ? Collections.<String> emptySet() : map.keySet());
  }

  /**
   * NOTE: This method is deprecated. Bring it back to keep backward compatible.
   * Replaced by StateModelFactory#getPartitionSet(String resourceName)
   * get partition set
   * @return partition key set
   */
  @Deprecated
  public Set<String> getPartitionSet() {
    // return union of all partitions, assuming partitionName is unique across all resources
    Set<String> allPartitions = new HashSet<String>();
    for (ConcurrentMap<String, T> map : _stateModelMap.values()) {
      allPartitions.addAll(map.keySet());
    }
    return allPartitions;
  }

  /**
   * create a default batch-message-wrapper for a resource
   * @param resourceName
   * @return batch message handler
   */
  public BatchMessageWrapper createBatchMessageWrapper(String resourceName) {
    return new BatchMessageWrapper();
  }

  /**
   * create a batch-message-wrapper for a resource and put it into map
   * @param resourceName
   * @return batch message handler
   */
  public BatchMessageWrapper createAndAddBatchMessageWrapper(String resourceName) {
    BatchMessageWrapper wrapper = createBatchMessageWrapper(resourceName);
    _batchMsgWrapperMap.put(resourceName, wrapper);
    return wrapper;
  }

  /**
   * get batch-message-wrapper for a resource
   * @param resourceName
   * @return batch message handler
   */
  public BatchMessageWrapper getBatchMessageWrapper(String resourceName) {
    return _batchMsgWrapperMap.get(resourceName);
  }

  /**
   * Get the customized thread pool to handle all state transition messages for the given resource.
   * If this method return null, Helix will use the shared thread pool to handle all messages.
   * This method may be called only once for per transition type per resource, it will NOT be called
   * during each state transition.
   *
   * @param resourceName
   * @return
   */
  public ExecutorService getExecutorService(String resourceName) {
    return null;
  }

  /**
   *Get thread pool to handle the given state transition (fromState->toState) callback for given
   * resource.
   *
   * If this method return null, the threadpool returned from
   * {\ref StateModelFactory.getExecutorService(String resourceName)} will be used. If that method
   * return null too, then the default shared threadpool will be used. This method may be called
   * only once for per transition type per resource, it will NOT be called during each state transition.
   *
   *
   * @param resourceName
   * @param fromState
   * @param toState
   * @return
   */
  public ExecutorService getExecutorService(String resourceName, String fromState, String toState) {
    return null;
  }
}
