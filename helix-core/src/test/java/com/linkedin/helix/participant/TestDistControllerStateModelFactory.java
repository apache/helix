package com.linkedin.helix.participant;

import org.testng.annotations.Test;
import org.testng.annotations.Test;

import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.participant.DistClusterControllerStateModel;
import com.linkedin.helix.participant.DistClusterControllerStateModelFactory;

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
