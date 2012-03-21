package com.linkedin.helix.util;

import java.util.Date;

import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;

public class TestZKClientPool
{

  @Test
  public void test() throws Exception
  {
    String testName = "TestZKClientPool";
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String zkAddr = "localhost:2187";
    ZkServer zkServer = TestHelper.startZkSever(zkAddr);
    ZkClient zkClient = ZKClientPool.getZkClient(zkAddr);
    
    zkClient.createPersistent("/" + testName, new ZNRecord(testName));
    ZNRecord record = zkClient.readData("/" + testName);
    Assert.assertEquals(record.getId(), testName);
    
    TestHelper.stopZkServer(zkServer);
    
    // restart zk 
    zkServer = TestHelper.startZkSever(zkAddr);
    try
    {
      zkClient = ZKClientPool.getZkClient(zkAddr);
      record = zkClient.readData("/" + testName);
      Assert.fail("should fail on zk no node exception");
    } catch (ZkNoNodeException e)
    {
      // OK
    } catch (Exception e)
    {
      Assert.fail("should not fail on exception other than ZkNoNodeException");
    }
    
    zkClient.createPersistent("/" + testName, new ZNRecord(testName));
    record = zkClient.readData("/" + testName);
    Assert.assertEquals(record.getId(), testName);
    
    zkClient.close();
    TestHelper.stopZkServer(zkServer);
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }
}
