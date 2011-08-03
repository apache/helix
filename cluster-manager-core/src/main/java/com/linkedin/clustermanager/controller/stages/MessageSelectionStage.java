package com.linkedin.clustermanager.controller.stages;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;

public class MessageSelectionStage extends AbstractBaseStage
{

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
  }
}
