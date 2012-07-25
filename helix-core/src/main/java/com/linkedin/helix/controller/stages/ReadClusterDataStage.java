/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.controller.stages;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.monitoring.mbeans.ClusterStatusMonitor;

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
    long processStartTime = System.currentTimeMillis();
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
    // System.out.println("ReadClusterDataStage done: "+(System.currentTimeMillis() - processStartTime));
  }
}
