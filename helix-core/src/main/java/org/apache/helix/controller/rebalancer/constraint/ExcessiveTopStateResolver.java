package org.apache.helix.controller.rebalancer.constraint;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.rebalancer.constraint.AbnormalStateResolver;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abnormal state resolver that gracefully fixes the abnormality of excessive top states for
 * single-topstate state model. For example, two replcias of a MasterSlave partition are assigned
 * with the Master state at the same time. This could be caused by a network partitioning or the
 * other unexpected issues.
 *
 * The resolver checks for the abnormality and computes recovery assignment which triggers the
 * rebalancer to eventually reset all the top state replias for once. After the resets, only one
 * replica will be assigned the top state.
 *
 * Note that without using this resolver, the regular Helix rebalance pipeline also removes the
 * excessive top state replicas. However, the default logic does not force resetting ALL the top
 * state replicas. Since the multiple top states situation may break application data, the default
 * resolution won't be enough to fix the potential problem.
 */
public class ExcessiveTopStateResolver implements AbnormalStateResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ExcessiveTopStateResolver.class);

  /**
   * The current states are not valid if there are more than 2 top state replicas for a single top
   * state state model.
   */
  @Override
  public boolean checkCurrentStates(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition, StateModelDefinition stateModelDef) {
    if (!stateModelDef.isSingleTopStateModel()) {
      return true;
    }
    if (currentStateOutput.getCurrentStateMap(resourceName, partition).values().stream()
        .filter(state -> state.equals(stateModelDef.getTopState())).count() > 1) {
      return false;
    }
    return true;
  }

  @Override
  public Map<String, String> computeRecoveryAssignment(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition, StateModelDefinition stateModelDef,
      List<String> preferenceList) {
    Map<String, String> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName, partition);
    if (checkCurrentStates(currentStateOutput, resourceName, partition, stateModelDef)) {
      // This method should not be triggered when the mapping is valid.
      // Log the warning for debug purposes.
      LOG.warn("The input current state map {} is valid, return the original current state.",
          currentStateMap);
      return currentStateMap;
    }

    Map<String, String> recoverMap = new HashMap<>(currentStateMap);
    String recoveryState = stateModelDef
        .getNextStateForTransition(stateModelDef.getTopState(), stateModelDef.getInitialState());

    // 1. We have to reset the expected top state replica host if it is hosting the top state
    // replica. Otherwise, the potential data issue will never be fixed there.
    if (preferenceList != null && !preferenceList.isEmpty()) {
      String expectedTopStateHost = preferenceList.get(0);
      if (recoverMap.get(expectedTopStateHost).equals(stateModelDef.getTopState())) {
        recoverMap.put(expectedTopStateHost, recoveryState);
      }
    }

    // 2. To minimize the impact of the resolution, we want to reserve one top state replica even
    // during the recovery process.
    boolean hasReservedTheTopState = false;
    for (String instance : recoverMap.keySet()) {
      if (recoverMap.get(instance).equals(stateModelDef.getTopState())) {
        if (hasReservedTheTopState) {
          recoverMap.put(instance, recoveryState);
        } else {
          hasReservedTheTopState = true;
        }
      }
    }
    // Here's what we expect to happen next:
    // 1. The partition assignment is changed to the proposed recovery state. Or it may be halfway
    // there.
    // 2. If the new current state is still invalid, then continue fixing it with the same logic.
    // 3. If the new current state contains only one top state replica, then we will hand it over
    // to the regular rebalancer logic. The rebalancer will trigger state transition to bring the
    // top state back to the expected allocation.
    // And the potential data issue will be fixed by then.
    return recoverMap;
  }
}
