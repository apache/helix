package com.linkedin.helix.controller.stages;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;

public class ReadHealthDataStage extends AbstractBaseStage
{
  private static final Logger logger = Logger
      .getLogger(ReadHealthDataStage.class.getName());
  HealthDataCache _cache;

  public ReadHealthDataStage()
  {
    _cache = new HealthDataCache();
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

    event.addAttribute("HealthDataCache", _cache);
  }
}
