package org.apache.helix.controller;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.controller.dataproviders.InstanceStateTransitionCapacityProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceWeightsDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;


/**
 * TODO: to be integrated to {@link org.apache.helix.controller.stages.IntermediateStateCalcStage}
 * A stateful throttler that tracks and uses pre-defined instance capacity for state transition throttling.
 * The instance capacity is given by {@link InstanceStateTransitionCapacityProvider} to quantify the headroom for extra
 * state transition on the instance, while resource-partition weights are given by {@link ResourceWeightsDataProvider}.
 * The throttler blocks bootstrapping state transition (from initial state) if the adding weight of the partition
 * exceed the instance remaining capacity.
 * This class holds internal state for instances that should last for each controller pipeline, the state should be
 * refreshed prior to the next pipeline to capture the new changes.
 */
public class CapacityBasedThrottler {

  // <instance, Map<attr, int>>
  protected final Map<String, Map<String, Integer>> _instanceRemainingCapacity = new HashMap<>();
  // ignore certain keys when doing the throttling
  private final Set<String> _keysToIgnore = new HashSet<>();
  private final ResourceControllerDataProvider _dataCache;
  private final InstanceStateTransitionCapacityProvider _instanceCapacityDataProvider;
  private final ResourceWeightsDataProvider _resourceWeightsDataProvider;

  public CapacityBasedThrottler(ResourceControllerDataProvider dataCache,
      InstanceStateTransitionCapacityProvider instanceCapacityDataProvider,
      ResourceWeightsDataProvider resourceWeightsDataProvider) {
    _dataCache = dataCache;
    _instanceCapacityDataProvider = instanceCapacityDataProvider;
    _resourceWeightsDataProvider = resourceWeightsDataProvider;
  }

  /**
   * Attempt to process the message and update instance capacity.
   * If the instance has capacity to take the message, update remaining capacity and return true;
   * otherwise, the capacity is not changed and return false, meaning the message is throttled.
   * This method is NOT idempotent.
   * @param instance instance for the attempted message
   * @param message the state transition message to throttle
   * @return true if message is accepted and capacity updated; false otherwise
   */
  public boolean tryProcessMessage(String instance, Message message) {
    if (getResourceInitialState(message.getResourceName()).equals(message.getFromState())) {
      // bootstrap new replica
      return charge(instance, message.getResourceName(), message.getPartitionName());
    }
    return true;
  }

  /**
   * Charge resource partition weights to the instance. This reduces the remaining capacity of the instance by applying
   * the weights of the resource partition.
   * If the instance has insufficient capacity for the partition, return false while no change made to internal state.
   * @param instance instance to throttle on
   * @param resource the resource of the message
   * @param partition resource partition of the message
   * @return true if the result is valid and applied; false if message is throttled
   */
  private boolean charge(String instance, String resource, String partition) {
    Map<String, Integer> weights =
        _resourceWeightsDataProvider.getPartitionWeights(resource, partition);
    Map<String, Integer> remains =
        _instanceRemainingCapacity.computeIfAbsent(instance,
            k -> new HashMap<>(_instanceCapacityDataProvider.getInstanceCapacity(k)));
    Map<String, Integer> newWeights = new HashMap<>();
    for (Map.Entry<String, Integer> entry : weights.entrySet()) {
      if (!remains.containsKey(entry.getKey())) {
        throw new IllegalArgumentException("Capacity key " + entry.getKey() + " is not specified in instance.");
      }
      int val = remains.get(entry.getKey()) - entry.getValue();
      if (val < 0 && !_keysToIgnore.contains(entry.getKey())) {
        return false;
      }
      newWeights.put(entry.getKey(), val);
    }
    remains.putAll(newWeights);
    return true;
  }

  private String getResourceInitialState(String resource) {
    IdealState idealState = _dataCache.getIdealState(resource);
    StateModelDefinition stateModelDefinition = _dataCache.getStateModelDef(idealState.getStateModelDefRef());
    return stateModelDefinition.getInitialState();
  }

  /**
   * Add a key so that the capacity constraint is ignored on this key for throttle computation.
   */
  public void addIgnoreKey(String key) {
    _keysToIgnore.add(key);
  }

  /**
   * Remove a key that was previously added to ignore, the constraint on this key is re-enabled.
   * No-op if the key wasn't ignored.
   */
  public void removeIgnoreKey(String key) {
    _keysToIgnore.remove(key);
  }
}
