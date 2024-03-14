package org.apache.helix.controller.rebalancer;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceRebalancer extends SemiAutoRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceRebalancer.class);

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
    LOG.info(String
        .format("Start computing ideal state for resource %s in maintenance mode.", resourceName));
    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName);
    if (currentStateMap == null || currentStateMap.size() == 0) {
      LOG.warn(String
          .format("No new partition will be assigned for %s in maintenance mode", resourceName));

      // Clear all preference lists, if the resource has not yet been rebalanced,
      // leave it as is
      for (List<String> pList : currentIdealState.getPreferenceLists().values()) {
        pList.clear();
      }
      return currentIdealState;
    }

    // One principal is to prohibit DROP -> OFFLINE and OFFLINE -> DROP state transitions.
    // Derived preference list from current state with state priority
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(currentIdealState.getStateModelDefRef());

    for (Partition partition : currentStateMap.keySet()) {
      Map<String, String> stateMap = currentStateMap.get(partition);
      List<String> preferenceList = new ArrayList<>(stateMap.keySet());

      // This sorting preserves the ordering of current state hosts in the order of current IS pref list
      Collections.sort(preferenceList, new PreferenceListNodeComparator(stateMap,
          stateModelDef, currentIdealState.getPreferenceList(partition.getPartitionName())));

      // This sorts the current state hosts based on the priority
      preferenceList.sort(new StatePriorityComparator(stateMap, stateModelDef));

      currentIdealState.setPreferenceList(partition.getPartitionName(), preferenceList);
    }
    LOG.info(String
        .format("End computing ideal state for resource %s in maintenance mode.", resourceName));
    return currentIdealState;
  }
}
