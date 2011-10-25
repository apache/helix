package com.linkedin.clustermanager.participant;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.ZkUnitTestBase;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class TestDistControllerStateModel extends ZkUnitTestBase
{
  final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  DistClusterControllerStateModel stateModel = null;

  @BeforeMethod(groups = { "unitTest" })
  public void beforeMethod()
  {
    stateModel = new DistClusterControllerStateModel(ZK_ADDR);
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }
    TestHelper.setupEmptyCluster(_zkClient, clusterName);
  }
  
  @Test(groups = { "unitTest" })
  public void testOnBecomeStandbyFromOffline()
  {
    stateModel.onBecomeStandbyFromOffline(null, null);
  }
  
  @Test(groups = { "unitTest" })
  public void testOnBecomeLeaderFromStandby()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");
    try
    {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }
  
  @Test(groups = { "unitTest" })
  public void testOnBecomeStandbyFromLeader()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }
  
  @Test(groups = { "unitTest" })
  public void testOnBecomeOfflineFromStandby()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");

    stateModel.onBecomeOfflineFromStandby(message, null);
  }
  
  @Test(groups = { "unitTest" })
  public void testOnBecomeDroppedFromOffline()
  {
    stateModel.onBecomeDroppedFromOffline(null, null);
  }

  @Test(groups = { "unitTest" })
  public void testOnBecomeOfflineFromDropped()
  {
    stateModel.onBecomeOfflineFromDropped(null, null);
  }
 
  @Test(groups = { "unitTest" })
  public void testRollbackOnError()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");
    try
    {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stateModel.rollbackOnError(message, new NotificationContext(null), null);
  }

  @Test(groups = { "unitTest" })
  public void testReset()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");
    try
    {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stateModel.reset();
  }

}
