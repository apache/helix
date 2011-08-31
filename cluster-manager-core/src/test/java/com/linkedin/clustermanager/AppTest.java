package com.linkedin.clustermanager;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Unit test for simple App.
 */
public class AppTest
{
  /**
   * Create the test case
   * 
   * @param testName
   *          name of the test case
   */
  public AppTest(String testName)
  {
  }

  /**
   * Rigourous Test :-)
   */
  @Test
  public void testApp()
  {
    AssertJUnit.assertTrue(true);
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
      String connectString = "esv4-app80.stg.linkedin.com:2181,esv4-app81.stg.linkedin.com:2181,esv4-app82.stg.linkedin.com:2181";
      ZooKeeper currentZookeeper = new ZooKeeper(connectString, 3000, watcher);
      Thread.sleep(3000);
      System.out.println("sessionId= " + currentZookeeper.getSessionId());
      System.out.println("sessionPassword= " + currentZookeeper.getSessionPasswd());

      
      Watcher watcherNew = new Watcher()
      {
        @Override
        public void process(WatchedEvent event)
        {
          System.out.println("In New connection event:" + event);
        }
      };
      ZooKeeper newZooKeeper = new ZooKeeper(connectString, 3000, watcherNew,currentZookeeper.getSessionId(),currentZookeeper.getSessionPasswd());
      System.out.println("sessionId= " + newZooKeeper.getSessionId());
     
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
