package org.apache.helix.healthcheck;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ReadHealthDataStage;
import org.apache.helix.controller.stages.StatsAggregationStage;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.monitoring.mbeans.ClusterAlertMBeanCollection;
import org.apache.helix.monitoring.mbeans.HelixStageLatencyMonitor;
import org.apache.log4j.Logger;

public class HealthStatsAggregator {
  private static final Logger LOG = Logger.getLogger(HealthStatsAggregator.class);

  public final static int DEFAULT_HEALTH_CHECK_LATENCY = 30 * 1000;

  private final HelixManager _manager;
  private final Pipeline _healthStatsAggregationPipeline;
  private final ClusterAlertMBeanCollection _alertItemCollection;
  private final Map<String, HelixStageLatencyMonitor> _stageLatencyMonitorMap =
      new HashMap<String, HelixStageLatencyMonitor>();

  public HealthStatsAggregator(HelixManager manager)
  {
    _manager = manager;

    // health stats pipeline
    _healthStatsAggregationPipeline = new Pipeline();
    _healthStatsAggregationPipeline.addStage(new ReadHealthDataStage());
    StatsAggregationStage statAggregationStage = new StatsAggregationStage();
    _healthStatsAggregationPipeline.addStage(statAggregationStage);
    _alertItemCollection = statAggregationStage.getClusterAlertMBeanCollection();

    registerStageLatencyMonitor(_healthStatsAggregationPipeline);
  }

  private void registerStageLatencyMonitor(Pipeline pipeline)
  {
    for (Stage stage : pipeline.getStages())
    {
      String stgName = stage.getStageName();
      if (!_stageLatencyMonitorMap.containsKey(stgName))
      {
        try
        {
          _stageLatencyMonitorMap.put(stage.getStageName(),
                                      new HelixStageLatencyMonitor(_manager.getClusterName(),
                                                                   stgName));
        }
        catch (Exception e)
        {
          LOG.error("Couldn't create StageLatencyMonitor mbean for stage: " + stgName, e);
        }
      }
      else
      {
        LOG.error("StageLatencyMonitor for stage: " + stgName
            + " already exists. Skip register it");
      }
    }
  }

  public synchronized void aggregate()
  {
    if (!isEnabled())
    {
      LOG.info("HealthAggregationTask is disabled.");
      return;
    }
    
    if (!_manager.isLeader())
    {
      LOG.error("Cluster manager: " + _manager.getInstanceName()
          + " is not leader. Pipeline will not be invoked");
      return;
    }

    try
    {
      ClusterEvent event = new ClusterEvent("healthChange");
      event.addAttribute("helixmanager", _manager);
      event.addAttribute("HelixStageLatencyMonitorMap", _stageLatencyMonitorMap);

      _healthStatsAggregationPipeline.handle(event);
      _healthStatsAggregationPipeline.finish();
    }
    catch (Exception e)
    {
      LOG.error("Exception while executing pipeline: " + _healthStatsAggregationPipeline,
                e);
    }
  }

  private boolean isEnabled()
  {
    ConfigAccessor configAccessor = _manager.getConfigAccessor();
    boolean enabled = true;
    if (configAccessor != null)
    {
      // zk-based cluster manager
      ConfigScope scope =
          new ConfigScopeBuilder().forCluster(_manager.getClusterName()).build();
      String isEnabled = configAccessor.get(scope, "healthChange.enabled");
      if (isEnabled != null)
      {
        enabled = new Boolean(isEnabled);
      }
    }
    else
    {
      LOG.debug("File-based cluster manager doesn't support disable healthChange");
    }
    return enabled;
  }
  
  public void init() {
    // Remove all the previous health check values, if any
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    List<String> existingHealthRecordNames = accessor.getChildNames(accessor.keyBuilder().healthReports(_manager.getInstanceName()));
    for(String healthReportName : existingHealthRecordNames)
    {
      LOG.info("Removing old healthrecord " + healthReportName);
      accessor.removeProperty(accessor.keyBuilder().healthReport(_manager.getInstanceName(),healthReportName));
    }

  }
  
  public void reset() {
    _alertItemCollection.reset();

    for (HelixStageLatencyMonitor stgLatencyMonitor : _stageLatencyMonitorMap.values())
    {
      stgLatencyMonitor.reset();
    }

  }
}
