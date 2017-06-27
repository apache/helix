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

import javax.management.JMException;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.monitoring.mbeans.ControllerStageMonitor;
import org.apache.log4j.Logger;

public class AbstractBaseStage implements Stage {
  private static Logger LOG = Logger.getLogger(AbstractBaseStage.class);

  protected ControllerStageMonitor _monitor;

  @Override
  public void init(StageContext context) {
    try {
      _monitor = new ControllerStageMonitor(getStageName());
    } catch (JMException e) {
      LOG.error("Error in creating ControllerStageMonitor for stage: " + getStageName(), e);
    }
  }

  @Override
  public void preProcess() {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(ClusterEvent event) throws Exception {

  }

  @Override
  public void postProcess() {

  }

  @Override
  public void release() {

  }

  @Override
  public String getStageName() {
    // default stage name will be the class name
    String className = this.getClass().getSimpleName();
    return className;
  }

  public void updateStageMonitorCounters(long latency) {
    if (_monitor != null) {
      _monitor.increaseProcessingCounter(latency);
    }
  }
}
