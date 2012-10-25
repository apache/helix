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
import org.apache.log4j.Logger;


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
    // DataAccessor dataAccessor = manager.getDataAccessor();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    _cache.refresh(accessor);

    event.addAttribute("HealthDataCache", _cache);

    long processLatency = System.currentTimeMillis() - startTime;
    addLatencyToMonitor(event, processLatency);
  }
}

