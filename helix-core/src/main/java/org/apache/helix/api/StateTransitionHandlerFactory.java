package org.apache.helix.api;

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

public abstract class StateTransitionHandlerFactory<T extends TransitionHandler> {
  /**
   * map from partitionId to transition-handler
   */
  private final ConcurrentMap<PartitionId, T> _transitionHandlerMap =
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
  public abstract T createStateTransitionHandler(PartitionId partitionId);

  /**
   * Create a state model for a partition
   * @param partitionId
   */
  public T createAndAddSTransitionHandler(PartitionId partitionId) {
    T stateModel = createStateTransitionHandler(partitionId);
    _transitionHandlerMap.put(partitionId, stateModel);
    return stateModel;
  }

  /**
   * Get the state model for a partition
   * @param partitionId
   * @return state model if exists, null otherwise
   */
  public T getTransitionHandler(PartitionId partitionId) {
    return _transitionHandlerMap.get(partitionId);
  }

  /**
   * remove state model for a partition
   * @param partitionId
   * @return state model removed or null if not exist
   */
  public T removeTransitionHandler(PartitionId partitionId) {
    return _transitionHandlerMap.remove(partitionId);
  }

  /**
   * get partition set
   * @return partitionId set
   */
  public Set<PartitionId> getPartitionSet() {
    return _transitionHandlerMap.keySet();
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
