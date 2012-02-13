package com.linkedin.helix.participant;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.DistClusterControllerStateModel;

public class TestDistControllerStateModel extends ZkUnitTestBase
{
  final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  DistClusterControllerStateModel stateModel = null;
	ZkClient _zkClient;

	@BeforeClass
	public void beforeClass()
	{
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
	}

	@AfterClass
	public void afterClass()
	{
		_zkClient.close();
	}

  @BeforeMethod()
  public void beforeMethod()
  {
    stateModel = new DistClusterControllerStateModel(ZK_ADDR);
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }
    TestHelper.setupEmptyCluster(_zkClient, clusterName);
  }

  @Test()
  public void testOnBecomeStandbyFromOffline()
  {
    stateModel.onBecomeStandbyFromOffline(null, null);
  }

  @Test()
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

  @Test()
  public void testOnBecomeStandbyFromLeader()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeOfflineFromStandby()
  {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setStateUnitKey(clusterName);
    message.setTgtName("controller_0");

    stateModel.onBecomeOfflineFromStandby(message, null);
  }

  @Test()
  public void testOnBecomeDroppedFromOffline()
  {
    stateModel.onBecomeDroppedFromOffline(null, null);
  }

  @Test()
  public void testOnBecomeOfflineFromDropped()
  {
    stateModel.onBecomeOfflineFromDropped(null, null);
  }

  @Test()
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

  @Test()
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
