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
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ContextId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.context.ControllerContext;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConfiguration;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ReadClusterDataStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(ReadClusterDataStage.class.getName());

  private ClusterDataCache _cache = null;

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    LOG.info("START ReadClusterDataStage.process()");

    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (cache == null && _cache == null) {
      cache = new ClusterDataCache();
    }
    _cache = cache;

    ClusterId clusterId = ClusterId.from(manager.getClusterName());
    ClusterAccessor clusterAccessor = new ClusterAccessor(clusterId, accessor, _cache);

    Cluster cluster = clusterAccessor.readCluster();

    // Update the cluster status gauges
    ClusterStatusMonitor clusterStatusMonitor =
        (ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor");
    if (clusterStatusMonitor != null) {
      Set<String> instanceSet = Sets.newHashSet();
      Set<String> liveInstanceSet = Sets.newHashSet();
      Set<String> disabledInstanceSet = Sets.newHashSet();
      Map<String, Set<String>> disabledPartitions = Maps.newHashMap();
      Map<String, Set<String>> tags = Maps.newHashMap();
      for (Participant participant : cluster.getParticipantMap().values()) {
        instanceSet.add(participant.getId().toString());
        if (participant.isAlive()) {
          liveInstanceSet.add(participant.getId().toString());
        }
        if (!participant.isEnabled()) {
          disabledInstanceSet.add(participant.getId().toString());
        }
        Set<String> partitionNames = Sets.newHashSet();
        for (PartitionId partitionId : participant.getDisabledPartitionIds()) {
          partitionNames.add(partitionId.toString());
        }
        disabledPartitions.put(participant.getId().toString(), partitionNames);
        tags.put(participant.getId().toString(), participant.getTags());
      }
      clusterStatusMonitor.setClusterInstanceStatus(liveInstanceSet, instanceSet,
          disabledInstanceSet, disabledPartitions, tags);
    }

    event.addAttribute("Cluster", cluster);
    event.addAttribute("ClusterDataCache", _cache);

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
