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

import java.util.Map;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Resource;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

public class ResourceValidationStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(ResourceValidationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("Cluster");
    if (cluster == null) {
      throw new StageException("Missing attributes in event:" + event + ". Requires Cluster");
    }
    Map<ResourceId, ResourceConfig> resourceConfigMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    if (resourceConfigMap == null) {
      throw new StageException("Resources must be computed prior to validation!");
    }
    Map<ResourceId, Resource> resourceMap = cluster.getResourceMap();
    Map<String, Map<String, String>> idealStateRuleMap =
        event.getAttribute(AttributeName.IDEAL_STATE_RULES.toString());

    for (ResourceId resourceId : resourceMap.keySet()) {
      // check every ideal state against the ideal state rules
      // the pipeline should not process any resources that have an unsupported ideal state
      IdealState idealState = resourceMap.get(resourceId).getIdealState();
      if (idealState == null) {
        continue;
      }
      if (idealStateRuleMap != null && !idealStateRuleMap.isEmpty()) {
        boolean hasMatchingRule = false;
        for (String ruleName : idealStateRuleMap.keySet()) {
          Map<String, String> rule = idealStateRuleMap.get(ruleName);
          boolean matches = idealStateMatchesRule(idealState, rule);
          hasMatchingRule = hasMatchingRule || matches;
          if (matches) {
            break;
          }
        }
        if (!hasMatchingRule) {
          LOG.warn("Resource " + resourceId + " does not have a valid ideal state!");
          resourceConfigMap.remove(resourceId);
        }
      }

      // check that every resource to process has a live state model definition
      StateModelDefId stateModelDefId = idealState.getStateModelDefId();
      StateModelDefinition stateModelDef = cluster.getStateModelMap().get(stateModelDefId);
      if (stateModelDef == null) {
        LOG.warn("Resource " + resourceId + " uses state model " + stateModelDefId
            + ", but it is not on the cluster!");
        resourceConfigMap.remove(resourceId);
      }
    }
  }

  /**
   * Check if the ideal state adheres to a rule
   * @param idealState the ideal state to check
   * @param rule the rules of a valid ideal state
   * @return true if the ideal state is a superset of the entries of the rule, false otherwise
   */
  private boolean idealStateMatchesRule(IdealState idealState, Map<String, String> rule) {
    Map<String, String> simpleFields = idealState.getRecord().getSimpleFields();
    for (String key : rule.keySet()) {
      String value = rule.get(key);
      if (!simpleFields.containsKey(key) || !value.equals(simpleFields.get(key))) {
        return false;
      }
    }
    return true;
  }
}
