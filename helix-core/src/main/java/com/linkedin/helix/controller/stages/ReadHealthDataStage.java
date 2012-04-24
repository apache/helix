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

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;

public class ReadHealthDataStage extends AbstractBaseStage
{
  private static final Logger LOG = Logger.getLogger(ReadHealthDataStage.class.getName());
  HealthDataCache _cache;

  public ReadHealthDataStage()
  {
    _cache = new HealthDataCache();
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    long startTime = System.currentTimeMillis();

    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null)
    {
      throw new StageException("HelixManager attribute value is null");
    }
    DataAccessor dataAccessor = manager.getDataAccessor();
    _cache.refresh(dataAccessor);

    event.addAttribute("HealthDataCache", _cache);

    long processLatency = System.currentTimeMillis() - startTime;
    addLatencyToMonitor(event, processLatency);
  }
}
