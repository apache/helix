package org.apache.helix.controller.pipeline;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline {
  private static final Logger logger = LoggerFactory.getLogger(Pipeline.class.getName());
  private final String _pipelineType;
  List<Stage> _stages;

  public Pipeline() {
    this("");
  }

  public Pipeline(String pipelineType) {
    _stages = new ArrayList<>();
    _pipelineType = pipelineType;
  }

  public void addStage(Stage stage) {
    _stages.add(stage);
    StageContext context = null;
    stage.init(context);
  }

  public String getPipelineType() {
    return _pipelineType;
  }

  public void handle(ClusterEvent event) throws Exception {
    if (_stages == null) {
      return;
    }
    for (Stage stage : _stages) {
      long startTime = System.currentTimeMillis();

      stage.preProcess();
      stage.process(event);
      stage.postProcess();

      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      logger.info(String.format("END %s for %s pipeline for cluster %s. took: %d ms for event %s",
          stage.getStageName(), _pipelineType, event.getClusterName(), duration,
          event.getEventId()));

      ClusterStatusMonitor clusterStatusMonitor =
          event.getAttribute(AttributeName.clusterStatusMonitor.name());
      if (clusterStatusMonitor != null) {
        clusterStatusMonitor.updateClusterEventDuration(stage.getStageName(), duration);
      }
    }
  }

  public void finish() {

  }

  public List<Stage> getStages() {
    return _stages;
  }
}
