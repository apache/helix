package com.linkedin.clustermanager.zk;

import org.I0Itec.zkclient.ZkServer;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.util.ZKClientPool;

public class ZkTestBase
{
  private static ZkServer _zkServer = null;
  protected static ZkClient _zkClient = null;

  protected static final String ZK_ADDR = "localhost:2183";
  protected static final String CLUSTER_PREFIX = "ESPRESSO_STORAGE";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";
  
  @BeforeSuite
  public void beforeSuite()
  {
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    
    _zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    AssertJUnit.assertTrue(_zkClient != null);
  }

  @AfterSuite
  public void afterSuite()
  {
    TestHelper.stopZkServer(_zkServer);
  }
  
  protected String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

}
