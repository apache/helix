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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;


public class ReadClusterDataStage extends AbstractBaseStage
{
  private static final Logger logger = Logger
      .getLogger(ReadClusterDataStage.class.getName());
  ClusterDataCache _cache;

  public ReadClusterDataStage()
  {
    _cache = new ClusterDataCache();
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    long startTime = System.currentTimeMillis();
    logger.info("START ReadClusterDataStage.process()");

    
    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null)
    {
      throw new StageException("HelixManager attribute value is null");
    }
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    _cache.refresh(dataAccessor);
    
    ClusterStatusMonitor clusterStatusMonitor = (ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor");
    if(clusterStatusMonitor != null)
    {
      int disabledInstances = 0;
      int disabledPartitions = 0;
      for(InstanceConfig  config : _cache._instanceConfigMap.values())
      {
        if(config.getInstanceEnabled() == false)
        {
          disabledInstances ++;
        }
        if(config.getDisabledPartitionMap() != null)
        {
          disabledPartitions += config.getDisabledPartitionMap().size();
        }
      }
      clusterStatusMonitor.setClusterStatusCounters(_cache._liveInstanceMap.size(), _cache._instanceConfigMap.size(), 
          disabledInstances, disabledPartitions);
    }

    event.addAttribute("ClusterDataCache", _cache);
    
    long endTime = System.currentTimeMillis();
    logger.info("END ReadClusterDataStage.process(). took: " + (endTime - startTime) + " ms");
  }
}
