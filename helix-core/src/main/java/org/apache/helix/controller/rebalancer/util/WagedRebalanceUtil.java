package org.apache.helix.controller.rebalancer.util;

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
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WagedRebalanceUtil {

  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalanceUtil.class);

  /**
   * @param clusterModel the cluster model that contains all the cluster status for the purpose of
   *                     rebalancing.
   * @return the new optimal assignment for the resources.
   */
  public static Map<String, ResourceAssignment> calculateAssignment(ClusterModel clusterModel,
      RebalanceAlgorithm algorithm) throws HelixRebalanceException {
    long startTime = System.currentTimeMillis();
    LOG.info("Start calculating for an assignment with algorithm {}",
        algorithm.getClass().getSimpleName());
    OptimalAssignment optimalAssignment = algorithm.calculate(clusterModel);
    Map<String, ResourceAssignment> newAssignment =
        optimalAssignment.getOptimalResourceAssignment();
    LOG.info("Finish calculating an assignment with algorithm {}. Took: {} ms.",
        algorithm.getClass().getSimpleName(), System.currentTimeMillis() - startTime);
    return newAssignment;
  }
}
