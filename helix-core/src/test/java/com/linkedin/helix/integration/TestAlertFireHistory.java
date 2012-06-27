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
package com.linkedin.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.healthcheck.HealthStatsAggregationTask;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

/**
 *
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class TestAlertFireHistory extends ZkStandAloneCMTestBase
{
  String _statName = "TestStat@DB=db1";
  String _stat = "TestStat";
  String metricName1 = "TestMetric1";
  String metricName2 = "TestMetric2";
  
  String _alertStr1 = "EXP(decay(1.0)(localhost_*.TestStat@DB=db1.TestMetric1))CMP(GREATER)CON(20)";
  String _alertStr2 = "EXP(decay(1.0)(localhost_*.TestStat@DB=db1.TestMetric2))CMP(GREATER)CON(100)";
  
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
  }
  
  @Test
  public void TestAlertDisable() throws InterruptedException
  {

    int[] metrics1 = {10, 15, 22, 24, 16};
    int[] metrics2 = {22, 115, 22, 141,16};
    setHealthData(metrics1, metrics2);
    
    String controllerName = CONTROLLER_PREFIX + "_0";
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;
    manager.startTimerTasks();
    
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr1);
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr2);
    
    ConfigScope scope = new ConfigScopeBuilder().forCluster(CLUSTER_NAME).build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("healthChange.enabled", "false");
    _setupTool.getClusterManagementTool().setConfig(scope, properties);
    
    HealthStatsAggregationTask task = new HealthStatsAggregationTask(_startCMResultMap.get(controllerName)._manager);
    task.run();
    Thread.sleep(100);
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();

    ZNRecord history = manager.getHelixDataAccessor().getProperty(keyBuilder.alertHistory()).getRecord();
    // 
    Assert.assertEquals(history, null);
    
    properties.put("healthChange.enabled", "true");
    _setupTool.getClusterManagementTool().setConfig(scope, properties);
    
    task.run();
    Thread.sleep(100);
    
    history = manager.getHelixDataAccessor().getProperty(keyBuilder.alertHistory()).getRecord();
    // 
    Assert.assertEquals(history.getMapFields().size(), 1);
  }
  
  @Test
  public void TestAlertHistory() throws InterruptedException
  {
    int[] metrics1 = {10, 15, 22, 24, 16};
    int[] metrics2 = {22, 115, 22, 141,16};
    setHealthData(metrics1, metrics2);

    String controllerName = CONTROLLER_PREFIX + "_0";
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;
    manager.stopTimerTasks();
    
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr1);
    _setupTool.getClusterManagementTool().addAlert(CLUSTER_NAME, _alertStr2);

    int historySize = 0;
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    ZNRecord history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    if(history != null)
    {
      historySize = history.getMapFields().size();
    }
    
    HealthStatsAggregationTask task = new HealthStatsAggregationTask(_startCMResultMap.get(controllerName)._manager);
    task.run();
    Thread.sleep(100);
    
    
    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    // 
    Assert.assertEquals(history.getMapFields().size(), 1 + historySize);
    TreeMap<String, Map<String, String>> recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll( history.getMapFields());
    Map<String, String> lastRecord = recordMap.firstEntry().getValue();
    Assert.assertTrue(lastRecord.size() == 4);
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
    
    setHealthData(metrics1, metrics2);
    task.run();
    Thread.sleep(100);
    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    // no change
    Assert.assertEquals(history.getMapFields().size(), 1 + historySize);
    recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll( history.getMapFields());
    lastRecord = recordMap.firstEntry().getValue();
    Assert.assertTrue(lastRecord.size() == 4);
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
    
    int [] metrics3 = {21, 44, 22, 14, 16};
    int [] metrics4 = {122, 115, 222, 41,16};
    setHealthData(metrics3, metrics4);
    task.run();
    Thread.sleep(100);
    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    // new delta should be recorded
    Assert.assertEquals(history.getMapFields().size(), 2 + historySize);
    recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll( history.getMapFields());
    lastRecord = recordMap.lastEntry().getValue();
    Assert.assertEquals(lastRecord.size(), 6);
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
    
    int [] metrics5 = {0, 0, 0, 0, 0};
    int [] metrics6 = {0, 0, 0, 0,0};
    setHealthData(metrics5, metrics6);
    task.run();
    Thread.sleep(100);
    history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
    // reset everything
    Assert.assertEquals(history.getMapFields().size(), 3 + historySize);
    recordMap = new TreeMap<String, Map<String, String>>();
    recordMap.putAll( history.getMapFields());
    lastRecord = recordMap.lastEntry().getValue();
    Assert.assertTrue(lastRecord.size() == 6);
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
    Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
    
    // Size of the history should be 30
    for(int i = 0;i < 27; i++)
    {
      int x = i % 2;
      int y = (i+1) % 2;
      int[] metricsx = {19 + 3*x, 19 + 3*y, 19 + 4*x, 18+4*y, 17+5*y};
      int[] metricsy = {99 + 3*x, 99 + 3*y, 98 + 4*x, 98+4*y, 97+5*y};
      
      setHealthData(metricsx, metricsy);
      task.run();
      Thread.sleep(100);
      history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
      
      Assert.assertEquals(history.getMapFields().size(), Math.min(3 + i + 1 + historySize, 30));
      recordMap = new TreeMap<String, Map<String, String>>();
      recordMap.putAll( history.getMapFields());
      lastRecord = recordMap.lastEntry().getValue();
      if(i == 0)
      {
        Assert.assertTrue(lastRecord.size() == 6);
        Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
      }
      else
      {
        Assert.assertTrue(lastRecord.size() == 10);
        if(x == 0)
        {
          Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        }
        else
        {
          Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
          Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
          Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        }
      }
    }
    // size limit is 30
    for(int i = 0;i < 10; i++)
    {
      int x = i % 2;
      int y = (i+1) % 2;
      int[] metricsx = {19 + 3*x, 19 + 3*y, 19 + 4*x, 18+4*y, 17+5*y};
      int[] metricsy = {99 + 3*x, 99 + 3*y, 98 + 4*x, 98+4*y, 97+5*y};
      
      setHealthData(metricsx, metricsy);
      task.run();
      Thread.sleep(100);
      history = helixDataAccessor.getProperty(keyBuilder.alertHistory()).getRecord();
      
      Assert.assertEquals(history.getMapFields().size(), 30);
      recordMap = new TreeMap<String, Map<String, String>>();
      recordMap.putAll( history.getMapFields());
      lastRecord = recordMap.lastEntry().getValue();
      
      Assert.assertEquals(lastRecord.size() , 10);
      if(x == 0)
      {
        Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
      }
      else
      {
        Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12922.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12920.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12921.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("OFF"));
        Assert.assertTrue(lastRecord.get("(localhost_12918.TestStat@DB#db1.TestMetric2)GREATER(100)").equals("ON"));
        Assert.assertTrue(lastRecord.get("(localhost_12919.TestStat@DB#db1.TestMetric1)GREATER(20)").equals("OFF"));
      }
    }
    
  }

}