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

import java.util.Map;

import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.monitoring.mbeans.HelixStageLatencyMonitor;

public class AbstractBaseStage implements Stage {
  @Override
  public void init(StageContext context) {

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
    String className = this.getClass().getName();
    return className;
  }

  public void addLatencyToMonitor(ClusterEvent event, long latency) {
    Map<String, HelixStageLatencyMonitor> stgLatencyMonitorMap =
        event.getAttribute("HelixStageLatencyMonitorMap");
    if (stgLatencyMonitorMap != null) {
      if (stgLatencyMonitorMap.containsKey(getStageName())) {
        HelixStageLatencyMonitor stgLatencyMonitor = stgLatencyMonitorMap.get(getStageName());
        stgLatencyMonitor.addStgLatency(latency);
      }
    }
  }
}
