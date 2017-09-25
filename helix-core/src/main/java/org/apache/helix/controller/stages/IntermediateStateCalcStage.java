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

import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.Resource;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * For partition compute the Intermediate State (instance,state) pair based on
 * the BestPossible State and Current State, with all constraints applied (such as state transition throttling).
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger logger = Logger.getLogger(IntermediateStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START Intermediate.process()");

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());

    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || bestPossibleStateOutput == null || resourceMap == null
        || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|BEST_POSSIBLE_STATE|RESOURCES|DataCache");
    }

    IntermediateStateOutput immediateStateOutput =
        compute(event, resourceMap, currentStateOutput, bestPossibleStateOutput);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), immediateStateOutput);

    long endTime = System.currentTimeMillis();
    logger.info("END ImmediateStateCalcStage.process(). took: " + (endTime - startTime) + " ms");
  }

  private IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput) {
    // for each resource
    // get the best possible state and current state
    // try to bring immediate state close to best possible state until
    // the possible pending state transition numbers reach the set throttle number.
    IntermediateStateOutput output = new IntermediateStateOutput();

    // TODO: add throttling logic here.
    for (String resourceName : resourceMap.keySet()) {
      logger.debug("Processing resource:" + resourceName);
      output.setState(resourceName, bestPossibleStateOutput.getPartitionStateMap(resourceName));
    }
    return output;
  }
}
