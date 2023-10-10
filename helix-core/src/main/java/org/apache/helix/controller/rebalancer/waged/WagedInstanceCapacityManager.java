package org.apache.helix.controller.rebalancer.waged;

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

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager class responsible for abstracting the management operations on the {@link WagedInstanceCapacity}
 * cache such as rebuild and update operations on various scenarios.
 */
public class WagedInstanceCapacityManager {

  private static final Logger LOG = LoggerFactory.getLogger(WagedInstanceCapacityManager.class);

  private static class SingletonHelper {
    private static final WagedInstanceCapacityManager INSTANCE = new WagedInstanceCapacityManager();
  }

  public static WagedInstanceCapacityManager getInstance() {
    return SingletonHelper.INSTANCE;
  }

  public void processEvent(ClusterEvent event, CurrentStateOutput currentStateOutput) {
    BaseControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    final Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    if (shouldNoOp(cache, resourceMap, event)) {
      return;
    }

    ResourceControllerDataProvider dataProvider = (ResourceControllerDataProvider) cache;
    Map<String, Resource> wagedEnabledResourceMap = resourceMap.entrySet()
        .stream()
        .filter(entry -> WagedValidationUtil.isWagedEnabled(cache.getIdealState(entry.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (wagedEnabledResourceMap.isEmpty()) {
      return;
    }

    // Phase 1: Rebuild Always
    // DONE: we only need to compute when there are resource using Waged. We should
    // do this as perf improvement in the future.
    WagedInstanceCapacity capacityProvider = new WagedInstanceCapacity(dataProvider);
    WagedResourceWeightsProvider weightProvider = new WagedResourceWeightsProvider(dataProvider);

    // Process the currentState and update the available instance capacity.
    capacityProvider.process(dataProvider, currentStateOutput, wagedEnabledResourceMap, weightProvider);
    dataProvider.setWagedCapacityProviders(capacityProvider, weightProvider);
  }

  /**
   * Function that checks whether we should return early, without any action on the capacity map or not.
   * @param cache it is the cluster level cache for the resources.
   * @param event the cluster event that is undergoing processing.
   * @return true, of the condition evaluate to true and no action is needed, else false.
   */
  static boolean shouldNoOp(BaseControllerDataProvider cache, Map<String, Resource> resourceMap, ClusterEvent event) {
    if (!(cache instanceof ResourceControllerDataProvider)) {
      return true;
    }

    if (resourceMap == null || resourceMap.isEmpty()) {
      return true;
    }

    ResourceControllerDataProvider dataProvider = (ResourceControllerDataProvider) cache;
    if (Objects.isNull(dataProvider.getWagedInstanceCapacity())) {
      return false;
    }

    switch (event.getEventType()) {
      case CustomizedStateChange:
      case CustomizedViewChange:
      case CustomizeStateConfigChange:
      case ExternalViewChange:
      case IdealStateChange:
      case OnDemandRebalance:
      case Resume:
      case RetryRebalance:
      case StateVerifier:
      case TargetExternalViewChange:
      case TaskCurrentStateChange:
        return true;
      default:
        return false;
    }
  }

}
