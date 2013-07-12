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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.integration.ZkStandAloneCMTestBaseWithPropertyServerCheck;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAlertActionTriggering extends
    ZkStandAloneCMTestBaseWithPropertyServerCheck
{
  String _statName = "TestStat@DB=db1";
  String _stat = "TestStat";
  String metricName1 = "TestMetric1";
  String metricName2 = "TestMetric2";
  void setHealthData(int[] val1, int[] val2)
  {
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      HelixManager manager = _startCMResultMap.get(instanceName)._manager;
      ZNRecord record = new ZNRecord(_stat);
      Map<String, String> valMap = new HashMap<String, String>();
      valMap.put(metricName1, val1[i] + "");
      valMap.put(metricName2, val2[i] + "");
      record.setSimpleField("TimeStamp", new Date().getTime() + "");
      record.setMapField(_statName, valMap);
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      Builder keyBuilder = helixDataAccessor.keyBuilder();
      helixDataAccessor
        .setProperty(keyBuilder.healthReport( manager.getInstanceName(), record.getId()), new HealthStat(record));
    }
    try
    {
      Thread.sleep(1000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  void setHealthData2(int[] val1)
  {
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      HelixManager manager = _startCMResultMap.get(instanceName)._manager;
      ZNRecord record = new ZNRecord(_stat);
      Map<String, String> valMap = new HashMap<String, String>();
      valMap.put(metricName2, val1[i] + "");
      record.setSimpleField("TimeStamp", new Date().getTime() + "");
      record.setMapField("TestStat@DB=TestDB;Partition=TestDB_3", valMap);
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      Builder keyBuilder = helixDataAccessor.keyBuilder();
      helixDataAccessor
        .setProperty(keyBuilder.healthReport( manager.getInstanceName(), record.getId()), new HealthStat(record));
    }
    try
    {
      Thread.sleep(1000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testAlertActionDisableNode() throws InterruptedException
  {
    // ConfigScope scope = new ConfigScopeBuilder().forCluster(CLUSTER_NAME).build();
    HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(CLUSTER_NAME).build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("healthChange.enabled", "true");
    _setupTool.getClusterManagementTool().setConfig(scope, properties);

    String alertStr1 = "EXP(decay(1.0)(localhost_*.TestStat@DB=db1.TestMetric1))CMP(GREATER)CON(20)ACTION(DISABLE_INSTANCE)";
    String alertStr2 = "EXP(decay(1.0)(localhost_*.TestStat@DB=db1.TestMetric2))CMP(GREATER)CON(120)ACTION(DISABLE_INSTANCE)";
    String alertStr3 = "EXP(decay(1.0)(localhost_*.TestStat@DB=TestDB;Partition=*.TestMetric2))CMP(GREATER)CON(160)ACTION(DISABLE_PARTITION)";

    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, alertStr1);
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, alertStr2);
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, alertStr3);

    int[] metrics1 = {10, 15, 22, 12, 16};
    int[] metrics2 = {22, 115, 22, 163,16};
    int[] metrics3 = {0, 0, 0, 0, 0};
    setHealthData(metrics1, metrics2);

    String controllerName = CONTROLLER_PREFIX + "_0";
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;

    HealthStatsAggregator task = new HealthStatsAggregator(_startCMResultMap.get(controllerName)._manager);
    task.aggregate();
    Thread.sleep(4000);
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 CLUSTER_NAME));
    Assert.assertTrue(result);

    Builder kb = manager.getHelixDataAccessor().keyBuilder();
    ExternalView externalView = manager.getHelixDataAccessor().getProperty(kb.externalView("TestDB"));
    // Test the DISABLE_INSTANCE alerts
    String participant1 = "localhost_" + (START_PORT + 3);
    String participant2 = "localhost_" + (START_PORT + 2);
    ConfigAccessor configAccessor = manager.getConfigAccessor();
    // scope = new ConfigScopeBuilder().forCluster(manager.getClusterName()).forParticipant(participant1).build();
    scope = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT)
                  .forCluster(manager.getClusterName())
                  .forParticipant(participant1)
                  .build();
    String isEnabled = configAccessor.get(scope, "HELIX_ENABLED");
    Assert.assertFalse(Boolean.parseBoolean(isEnabled));

    // scope = new ConfigScopeBuilder().forCluster(manager.getClusterName()).forParticipant(participant2).build();
    scope = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT)
                  .forCluster(manager.getClusterName())
                  .forParticipant(participant2)
                  .build();
    isEnabled = configAccessor.get(scope, "HELIX_ENABLED");
    Assert.assertFalse(Boolean.parseBoolean(isEnabled));

    for(String partitionName : externalView.getRecord().getMapFields().keySet())
    {
      for(String hostName :  externalView.getRecord().getMapField(partitionName).keySet())
      {
        if(hostName.equals(participant1) || hostName.equals(participant2))
        {
          Assert.assertEquals(externalView.getRecord().getMapField(partitionName).get(hostName), "OFFLINE");
        }
      }
    }

    // enable the disabled instances
    setHealthData(metrics3, metrics3);
    task.aggregate();
    Thread.sleep(1000);

    manager.getClusterManagmentTool().enableInstance(manager.getClusterName(), participant2, true);
    manager.getClusterManagmentTool().enableInstance(manager.getClusterName(), participant1, true);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 CLUSTER_NAME));
    Assert.assertTrue(result);

    // Test the DISABLE_PARTITION case
    int[] metrics4 = {22, 115, 22, 16,163};
    setHealthData2(metrics4);
    task.aggregate();

    // scope = new ConfigScopeBuilder().forCluster(manager.getClusterName()).forParticipant(participant1).build();
    scope = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT)
                .forCluster(manager.getClusterName())
                .forParticipant(participant1)
                .build();
    isEnabled = configAccessor.get(scope, "HELIX_ENABLED");
    Assert.assertTrue(Boolean.parseBoolean(isEnabled));

    // scope = new ConfigScopeBuilder().forCluster(manager.getClusterName()).forParticipant(participant2).build();
    scope = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT)
                .forCluster(manager.getClusterName())
                .forParticipant(participant2)
                .build();
    isEnabled = configAccessor.get(scope, "HELIX_ENABLED");
    Assert.assertTrue(Boolean.parseBoolean(isEnabled));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 CLUSTER_NAME));
    Assert.assertTrue(result);
    String participant3 = "localhost_" + (START_PORT + 4);
    externalView = manager.getHelixDataAccessor().getProperty(kb.externalView("TestDB"));
    Assert.assertTrue(externalView.getRecord().getMapField("TestDB_3").get(participant3).equalsIgnoreCase("OFFLINE"));

    InstanceConfig nodeConfig =
        helixDataAccessor.getProperty(keyBuilder.instanceConfig(participant3));
    Assert.assertTrue(
        nodeConfig.getRecord().getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString())
          .contains("TestDB_3"));

  }
}
