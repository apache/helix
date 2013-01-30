package org.apache.helix.manager.zk;

import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.integration.ZkStandAloneCMTestBaseWithPropertyServerCheck;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class TestZkStateChangeListener extends ZkStandAloneCMTestBaseWithPropertyServerCheck
{
  @Test
  public void testDisconnectHistory() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    StartCMResult controllerResult = _startCMResultMap.get(controllerName);
    ZKHelixManager controller = (ZKHelixManager) controllerResult._manager;
    ZkStateChangeListener listener1 = new ZkStateChangeListener(controller, 5000, 10);
    // 11 disconnects in 5 sec
    for(int i = 0;i < 11; i++)
    {
      Thread.sleep(200);
      listener1.handleStateChanged(KeeperState.Disconnected);
      if(i<10)
      {
        Assert.assertTrue(controller.isConnected());
      }
      else
      {
        Assert.assertFalse(controller.isConnected());
      }
    }
    
    // If maxDisconnectThreshold is 0 it should be set to 1
    String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    ZKHelixManager manager = (ZKHelixManager) _startCMResultMap.get(instanceName)._manager;
    
    ZkStateChangeListener listener2 = new ZkStateChangeListener(manager, 5000, 0);
    for(int i = 0;i < 2; i++)
    {
      Thread.sleep(200);
      listener2.handleStateChanged(KeeperState.Disconnected);
      if(i < 1)
      {
        Assert.assertTrue(manager.isConnected());
      }
      else
      {
        Assert.assertFalse(manager.isConnected());
      }
    }
    
    // If there are long time after disconnect, older history should be cleanup
    instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 1);
    manager = (ZKHelixManager) _startCMResultMap.get(instanceName)._manager;
    
    ZkStateChangeListener listener3 = new ZkStateChangeListener(manager, 5000, 5);
    for(int i = 0;i < 3; i++)
    {
      Thread.sleep(200);
      listener3.handleStateChanged(KeeperState.Disconnected);
      Assert.assertTrue(manager.isConnected());
    }
    Thread.sleep(5000);
    // Old entries should be cleaned up
    for(int i = 0;i < 3; i++)
    {
      Thread.sleep(200);
      listener3.handleStateChanged(KeeperState.Disconnected);
      Assert.assertTrue(manager.isConnected());
    }
    for(int i = 0;i < 2; i++)
    {
      Thread.sleep(200);
      listener3.handleStateChanged(KeeperState.Disconnected);
      Assert.assertTrue(manager.isConnected());
    }
    listener3.handleStateChanged(KeeperState.Disconnected);
    Assert.assertFalse(manager.isConnected());
  }
}
