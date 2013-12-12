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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ContextId;
import org.apache.helix.controller.context.ControllerContext;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConfiguration;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class ReadClusterDataStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(ReadClusterDataStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    LOG.info("START ReadClusterDataStage.process()");

    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ClusterId clusterId = ClusterId.from(manager.getClusterName());
    ClusterAccessor clusterAccessor = new ClusterAccessor(clusterId, accessor);

    Cluster cluster = clusterAccessor.readCluster();

    ClusterStatusMonitor clusterStatusMonitor =
        (ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor");
    if (clusterStatusMonitor != null) {
      // TODO fix it
      // int disabledInstances = 0;
      // int disabledPartitions = 0;
      // for (InstanceConfig config : _cache._instanceConfigMap.values()) {
      // if (config.getInstanceEnabled() == false) {
      // disabledInstances++;
      // }
      // if (config.getDisabledPartitions() != null) {
      // disabledPartitions += config.getDisabledPartitions().size();
      // }
      // }
      // clusterStatusMonitor.setClusterStatusCounters(_cache._liveInstanceMap.size(),
      // _cache._instanceConfigMap.size(), disabledInstances, disabledPartitions);
    }

    event.addAttribute("ClusterDataCache", cluster);

    // read contexts (if any)
    Map<ContextId, ControllerContext> persistedContexts = null;
    if (cluster != null) {
      persistedContexts = cluster.getContextMap();
    } else {
      persistedContexts = Maps.newHashMap();
    }
    ControllerContextProvider contextProvider = new ControllerContextProvider(persistedContexts);
    event.addAttribute(AttributeName.CONTEXT_PROVIDER.toString(), contextProvider);

    // read ideal state rules (if any)
    ClusterConfiguration clusterConfiguration =
        accessor.getProperty(accessor.keyBuilder().clusterConfig());
    if (clusterConfiguration == null) {
      clusterConfiguration = new ClusterConfiguration(cluster.getId());
    }
    event.addAttribute(AttributeName.IDEAL_STATE_RULES.toString(),
        clusterConfiguration.getIdealStateRules());

    long endTime = System.currentTimeMillis();
    LOG.info("END ReadClusterDataStage.process(). took: " + (endTime - startTime) + " ms");
  }
}
