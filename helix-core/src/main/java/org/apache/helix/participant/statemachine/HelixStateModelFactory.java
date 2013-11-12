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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.messaging.handling.BatchMessageWrapper;

/**
 * State model factory that uses concrete id classes instead of strings.
 * Replacing {@link org.apache.helix.participant.statemachine.StateModelFactory}
 */
public abstract class HelixStateModelFactory<T extends StateModel> {
  /**
   * map from partitionId to stateModel
   */
  private final ConcurrentMap<PartitionId, T> _stateModelMap =
      new ConcurrentHashMap<PartitionId, T>();

  /**
   * map from resourceName to BatchMessageWrapper
   */
  private final ConcurrentMap<ResourceId, BatchMessageWrapper> _batchMsgWrapperMap =
      new ConcurrentHashMap<ResourceId, BatchMessageWrapper>();

  /**
   * This method will be invoked only once per partition per session
   * @param partitionId
   * @return
   */
  public abstract T createNewStateModel(PartitionId partitionId);

  /**
   * Create a state model for a partition
   * @param partitionId
   */
  public T createAndAddStateModel(PartitionId partitionId) {
    T stateModel = createNewStateModel(partitionId);
    _stateModelMap.put(partitionId, stateModel);
    return stateModel;
  }

  /**
   * Get the state model for a partition
   * @param partitionId
   * @return state model if exists, null otherwise
   */
  public T getStateModel(PartitionId partitionId) {
    return _stateModelMap.get(partitionId);
  }

  /**
   * remove state model for a partition
   * @param partitionId
   * @return state model removed or null if not exist
   */
  public T removeStateModel(PartitionId partitionId) {
    return _stateModelMap.remove(partitionId);
  }

  /**
   * get partition set
   * @return partitionId set
   */
  public Set<PartitionId> getPartitionSet() {
    return _stateModelMap.keySet();
  }

  /**
   * create a default batch-message-wrapper for a resource
   * @param resourceId
   * @return
   */
  public BatchMessageWrapper createBatchMessageWrapper(ResourceId resourceId) {
    return new BatchMessageWrapper();
  }

  /**
   * create a batch-message-wrapper for a resource and put it into map
   * @param resourceId
   * @return
   */
  public BatchMessageWrapper createAndAddBatchMessageWrapper(ResourceId resourceId) {
    BatchMessageWrapper wrapper = createBatchMessageWrapper(resourceId);
    _batchMsgWrapperMap.put(resourceId, wrapper);
    return wrapper;
  }

  /**
   * get batch-message-wrapper for a resource
   * @param resourceId
   * @return
   */
  public BatchMessageWrapper getBatchMessageWrapper(ResourceId resourceId) {
    return _batchMsgWrapperMap.get(resourceId);
  }
}
