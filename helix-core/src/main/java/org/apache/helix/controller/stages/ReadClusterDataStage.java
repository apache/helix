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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ReadClusterDataStage extends AbstractBaseStage {
  private static final Logger logger = Logger.getLogger(ReadClusterDataStage.class.getName());

  private ClusterDataCache _cache = null;

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START ReadClusterDataStage.process()");

    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }

    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    if (cache == null && _cache == null) {
      cache = new ClusterDataCache(event.getClusterName());
    }
    _cache = cache;

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    _cache.refresh(dataAccessor);
    if (!_cache.isTaskCache()) {
      final ClusterStatusMonitor clusterStatusMonitor =
          event.getAttribute(AttributeName.clusterStatusMonitor.name());
      asyncExecute(_cache.getAsyncTasksThreadPool(), new Callable<Object>() {
        @Override public Object call() {
          // Update the cluster status gauges
          if (clusterStatusMonitor != null) {
            logger.debug("Update cluster status monitors");

            Set<String> instanceSet = Sets.newHashSet();
            Set<String> liveInstanceSet = Sets.newHashSet();
            Set<String> disabledInstanceSet = Sets.newHashSet();
            Map<String, Map<String, List<String>>> disabledPartitions = Maps.newHashMap();
            Map<String, List<String>> oldDisabledPartitions = Maps.newHashMap();
            Map<String, Set<String>> tags = Maps.newHashMap();
            Map<String, LiveInstance> liveInstanceMap = _cache.getLiveInstances();
            for (Map.Entry<String, InstanceConfig> e : _cache.getInstanceConfigMap().entrySet()) {
              String instanceName = e.getKey();
              InstanceConfig config = e.getValue();
              instanceSet.add(instanceName);
              if (liveInstanceMap.containsKey(instanceName)) {
                liveInstanceSet.add(instanceName);
              }
              if (!config.getInstanceEnabled()) {
                disabledInstanceSet.add(instanceName);
              }

              // TODO : Get rid of this data structure once the API is removed.
              oldDisabledPartitions.put(instanceName, config.getDisabledPartitions());
              disabledPartitions.put(instanceName, config.getDisabledPartitionsMap());

              Set<String> instanceTags = Sets.newHashSet(config.getTags());
              tags.put(instanceName, instanceTags);
            }
            clusterStatusMonitor
                .setClusterInstanceStatus(liveInstanceSet, instanceSet, disabledInstanceSet,
                    disabledPartitions, oldDisabledPartitions, tags);
            logger.debug("Complete cluster status monitors update.");
          }
          return null;
        }
      });
    }
    event.addAttribute(AttributeName.ClusterDataCache.name(), _cache);

    long endTime = System.currentTimeMillis();
    logger.info("END " + GenericHelixController.getPipelineType(_cache.isTaskCache())
        + " ReadClusterDataStage.process(). took: " + (endTime - startTime) + " ms");
  }
}
