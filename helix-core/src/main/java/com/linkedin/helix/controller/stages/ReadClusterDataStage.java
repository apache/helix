package com.linkedin.helix.controller.stages;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
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
    DataAccessor dataAccessor = manager.getDataAccessor();
    _cache.refresh(dataAccessor);
    
    ClusterStatusMonitor clusterStatusMonitor = (ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor");
    if(clusterStatusMonitor != null)
    {
      clusterStatusMonitor.setLiveInstanceNum(_cache._liveInstanceMap.size(), _cache._instanceConfigMap.size());
    }

    event.addAttribute("ClusterDataCache", _cache);
    // System.out.println("ReadClusterDataStage done: "+(System.currentTimeMillis() - processStartTime));
  }
}
