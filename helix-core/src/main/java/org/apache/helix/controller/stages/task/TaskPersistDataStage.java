package org.apache.helix.controller.stages.task;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskPersistDataStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(TaskPersistDataStage.class);

  @Override
  public void process(ClusterEvent event) {
    LOG.info("START TaskPersistDataStage.process()");
    long startTime = System.currentTimeMillis();

    // Persist partition assignment of resources.
    WorkflowControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    cache.getTaskDataCache().persistDataChanges(manager.getHelixDataAccessor());

    long endTime = System.currentTimeMillis();
    LOG.info("END TaskPersistDataStage.process() for cluster {} took {} ms", cache.getClusterName(),
        (endTime - startTime));
  }
}
