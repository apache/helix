package org.apache.helix.controller.stages;

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
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * Check and invoke custom implementation idealstate rebalancers.<br/>
 * If the resourceConfig has specified className of the customized rebalancer, <br/>
 * the rebalancer will be invoked to re-write the idealstate of the resource<br/>
 */
@Deprecated
public class RebalanceIdealStateStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(RebalanceIdealStateStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute("helixmanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, IdealState> idealStateMap = cache.getIdealStates();
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());

    Map<String, IdealState> updatedIdealStates = new HashMap<String, IdealState>();
    for (String resourceName : idealStateMap.keySet()) {
      IdealState currentIdealState = idealStateMap.get(resourceName);
      if (currentIdealState.getRebalanceMode() == RebalanceMode.USER_DEFINED
          && currentIdealState.getRebalancerRef() != null) {
        String rebalancerClassName = currentIdealState.getRebalancerRef().toString();
        LOG.info("resource " + resourceName + " use idealStateRebalancer " + rebalancerClassName);
        try {
          Rebalancer balancer =
              (Rebalancer) (HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());
          balancer.init(manager);
          Resource resource = new Resource(resourceName);
          for (String partitionName : currentIdealState.getPartitionSet()) {
            resource.addPartition(partitionName);
          }
          ResourceAssignment resourceAssignment =
              balancer.computeResourceMapping(resource, currentIdealState, currentStateOutput,
                  cache);
          StateModelDefinition stateModelDef =
              cache.getStateModelDef(currentIdealState.getStateModelDefRef());
          currentIdealState.updateFromAssignment(resourceAssignment, stateModelDef);
          updatedIdealStates.put(resourceName, currentIdealState);
        } catch (Exception e) {
          LOG.error("Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
        }
      }
    }
    if (updatedIdealStates.size() > 0) {
      cache.getIdealStates().putAll(updatedIdealStates);
    }
  }
}
