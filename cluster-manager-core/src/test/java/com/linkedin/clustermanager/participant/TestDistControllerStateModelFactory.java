package com.linkedin.clustermanager.participant;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.ZkUnitTestBase;

public class TestDistControllerStateModelFactory
{
  final String zkAddr = ZkUnitTestBase.ZK_ADDR;
      
  @Test(groups = { "unitTest" })
  public void testDistControllerStateModelFactory()
  {
    DistClusterControllerStateModelFactory factory = new DistClusterControllerStateModelFactory(zkAddr);
    DistClusterControllerStateModel stateModel = factory.createNewStateModel("key");
    stateModel.onBecomeStandbyFromOffline(null, null);
  }
}
