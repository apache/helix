package com.linkedin.clustermanager.controller.stages;

import com.linkedin.clustermanager.pipeline.AbstractBaseStage;

public class ReadClusterDataStage extends AbstractBaseStage
{
  ClusterDataCache _cache;

  public ReadClusterDataStage()
  {
    _cache = new ClusterDataCache();
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    event.addAttribute("clusterDataCache", _cache);
  }

}
