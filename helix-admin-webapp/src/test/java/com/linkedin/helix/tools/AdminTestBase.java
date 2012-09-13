package com.linkedin.helix.tools;

import java.util.logging.Level;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.AdminTestHelper.AdminThread;
import com.linkedin.helix.util.ZKClientPool;

public class AdminTestBase
{
  private static Logger      LOG        = Logger.getLogger(AdminTestBase.class);
  public static final String ZK_ADDR    = "localhost:2187";
  final static int           ADMIN_PORT = 2202;

  protected static ZkServer  _zkServer;
  protected static ZkClient  _gZkClient;
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

    // start admin
    _adminThread = new AdminThread(ZK_ADDR, ADMIN_PORT);
    _adminThread.start();
  }

  @AfterSuite
  public void afterSuite()
  {
    // stop zk
    ZKClientPool.reset();
    _gZkClient.close();

    TestHelper.stopZkServer(_zkServer);

    // stop admin
    _adminThread.stop();
  }

}
