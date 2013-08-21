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

import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.log4j.Logger;

public class Pipeline {
  private static final Logger logger = Logger.getLogger(Pipeline.class.getName());
  List<Stage> _stages;

  public Pipeline() {
    _stages = new ArrayList<Stage>();
  }

  public void addStage(Stage stage) {
    _stages.add(stage);
    StageContext context = null;
    stage.init(context);
  }

  public void handle(ClusterEvent event) throws Exception {
    if (_stages == null) {
      return;
    }
    for (Stage stage : _stages) {
      stage.preProcess();
      stage.process(event);
      stage.postProcess();
    }
  }

  public void finish() {

  }

  public List<Stage> getStages() {
    return _stages;
  }
}
