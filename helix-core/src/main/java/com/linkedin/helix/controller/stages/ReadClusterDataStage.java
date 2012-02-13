package com.linkedin.helix.controller.stages;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;

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
    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null)
    {
      throw new StageException("HelixManager attribute value is null");
    }
    DataAccessor dataAccessor = manager.getDataAccessor();
    _cache.refresh(dataAccessor);

    event.addAttribute("ClusterDataCache", _cache);
  }
}
