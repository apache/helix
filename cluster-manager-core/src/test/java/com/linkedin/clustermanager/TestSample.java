package com.linkedin.clustermanager;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;

/**
 * 
 * @author kgopalak
 * 
 */

public class TestSample
{

  @Test (groups = {"unitTest"})
  public final void testCallbackHandler()
  {
    ZKDataAccessor client = null;
    String path = null;
    Object listener = null;
    EventType[] eventTypes = null;

  }

  @BeforeMethod (groups = {"unitTest"})
  public void asd()
  {
    System.out.println("In Set up");
  }

  @Test (groups = {"unitTest"})
  public void testB()
  {
    System.out.println("In method testB");

  }

  @Test (groups = {"unitTest"})
  public void testA()
  {
    System.out.println("In method testA");

  }

  @AfterMethod (groups = {"unitTest"})
  public void sfds()
  {
    System.out.println("In tear down");
  }
}
