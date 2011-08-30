package com.linkedin.clustermanager;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkClientWrapper extends ZKBaseTest
{
  @Test
  void testGetStat()
  {
    String path = "/tmp/getStatTest";
    _zkClient.deleteRecursive(path);

    Stat stat, newStat;
    stat = _zkClient.getStat(path);
    Assert.assertNull(stat);
    _zkClient.createPersistent(path, true);

    stat = _zkClient.getStat(path);
    Assert.assertNotNull(stat);

    newStat = _zkClient.getStat(path);
    Assert.assertEquals(stat, newStat);

    _zkClient.writeData(path, "Test".getBytes());
    newStat = _zkClient.getStat(path);
    Assert.assertNotSame(stat, newStat);
  }

  //@Test
  void testSessioExpire()
  {
    IZkStateListener listener = new IZkStateListener()
    {

      @Override
      public void handleStateChanged(KeeperState state) throws Exception
      {
        ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
        ZooKeeper zookeeper = connection.getZookeeper();
        System.out.println("current sessionId= " + zookeeper.getSessionId());
        System.out.println("In Old connection New state " + state);
      }

      @Override
      public void handleNewSession() throws Exception
      {
        System.out.println("Old connection New session");
      }
    };
    _zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
    ZooKeeper zookeeper = connection.getZookeeper();
    System.out.println("old sessionId= " + zookeeper.getSessionId());
    try
    {
      Watcher watcher = new Watcher()
      {
        @Override
        public void process(WatchedEvent event)
        {
          System.out.println("In New connection event:" + event);
        }
      };
      ZooKeeper newZookeeper =
          new ZooKeeper(connection.getServers(),
                        zookeeper.getSessionTimeout(),
                        watcher,
                        zookeeper.getSessionId(),
                        zookeeper.getSessionPasswd());
      System.out.println("New sessionId= " + newZookeeper.getSessionId());
      Thread.sleep(9000);
      System.out.println("After sleep");
      newZookeeper.close();
      Thread.sleep(10000);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testSessionExpiryWithZK()
  {
    try
    {
      Watcher watcher = new Watcher()
      {
        @Override
        public void process(WatchedEvent event)
        {
          System.out.println("In Old connection event:" + event);
        }
      };
      ZooKeeper currentZookeeper = new ZooKeeper("localhost:2029", 3000, watcher);
      Thread.sleep(3000);
      System.out.println("old sessionId= " + currentZookeeper.getSessionId());
      System.out.println("old sessionPassword= " + currentZookeeper.getSessionPasswd());

      
      Watcher watcherNew = new Watcher()
      {
        @Override
        public void process(WatchedEvent event)
        {
          System.out.println("In New connection event:" + event);
        }
      };
      ZooKeeper newZooKeeper = new ZooKeeper("localhost:2029", 3000, watcherNew,currentZookeeper.getSessionId(),currentZookeeper.getSessionPasswd());
      Thread.sleep(3000);
      System.out.println("new sessionId= " + newZooKeeper.getSessionId());
     
      newZooKeeper.close();
      Thread.sleep(10000);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
