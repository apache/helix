package org.apache.helix.tools;

import java.util.logging.Level;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.AdminTestHelper.AdminThread;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


public class AdminTestBase
{
  private static Logger      LOG        = Logger.getLogger(AdminTestBase.class);
  public static final String ZK_ADDR    = "localhost:2187";
  protected final static int           ADMIN_PORT = 2202;

  protected static ZkServer  _zkServer;
  protected static ZkClient  _gZkClient;
  protected static ClusterSetup _gSetupTool;

  static AdminThread         _adminThread;

  @BeforeSuite
  public void beforeSuite() throws Exception
  {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    // start zk
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _gZkClient = new ZkClient(ZK_ADDR);
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
    _gSetupTool = new ClusterSetup(ZK_ADDR);
    
    // start admin
    _adminThread = new AdminThread(ZK_ADDR, ADMIN_PORT);
    _adminThread.start();
    
    // wait for the web service to start
    Thread.sleep(100);
  }

  @AfterSuite
  public void afterSuite()
  {
    // System.out.println("START AdminTestBase.afterSuite() at " + new Date(System.currentTimeMillis()));
    // stop admin
    _adminThread.stop();
    
    // stop zk
    ZKClientPool.reset();
    _gZkClient.close();

    TestHelper.stopZkServer(_zkServer);
    // System.out.println("END AdminTestBase.afterSuite() at " + new Date(System.currentTimeMillis()));
  }

}
