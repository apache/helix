package com.linkedin.helix.controller.stages;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;

public class TestParseInfoFromAlert extends ZkStandAloneCMTestBase
{
  @Test
  public void TestParse()
  {
    StatsAggregationStage stage = new StatsAggregationStage();
    String controllerName = CONTROLLER_PREFIX + "_0";
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;
    
    String instanceName  = stage.parseInstanceName("localhost_12918.TestStat@DB=123.latency", manager);
    Assert.assertTrue(instanceName.equals("localhost_12918"));
    
    instanceName  = stage.parseInstanceName("localhost_12955.TestStat@DB=123.latency", manager);
    Assert.assertTrue(instanceName == null);
    
    
    instanceName  = stage.parseInstanceName("localhost_12922.TestStat@DB=123.latency", manager);
    Assert.assertTrue(instanceName.equals("localhost_12922"));
    
    

    String resourceName  = stage.parseResourceName("localhost_12918.TestStat@DB=TestDB.latency", manager);
    Assert.assertTrue(resourceName.equals("TestDB"));
    

    String partitionName  = stage.parsePartitionName("localhost_12918.TestStat@DB=TestDB,Partition=TestDB_22.latency", manager);
    Assert.assertTrue(partitionName.equals("TestDB_22"));
  }
}
