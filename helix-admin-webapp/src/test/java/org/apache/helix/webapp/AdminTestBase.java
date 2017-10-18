package org.apache.helix.webapp;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.logging.Level;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.helix.webapp.AdminTestHelper.AdminThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class AdminTestBase {
  private static Logger LOG = LoggerFactory.getLogger(AdminTestBase.class);
  public static final String ZK_ADDR = "localhost:2187";
  protected final static int ADMIN_PORT = 2202;

  protected static ZkServer _zkServer;
  protected static ZkClient _gZkClient;
  protected static ClusterSetup _gSetupTool;
  protected static Client _gClient;

  static AdminThread _adminThread;

  @BeforeSuite
  public void beforeSuite() throws Exception {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    // start zk
    _zkServer = TestHelper.startZkServer(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _gZkClient =
        new ZkClient(ZK_ADDR, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            ZkClient.DEFAULT_SESSION_TIMEOUT, new ZNRecordSerializer());
    _gSetupTool = new ClusterSetup(_gZkClient);

    // start admin
    _adminThread = new AdminThread(ZK_ADDR, ADMIN_PORT);
    _adminThread.start();

    // create a client
    _gClient = new Client(Protocol.HTTP);

    // wait for the web service to start
    Thread.sleep(100);
  }

  @AfterSuite
  public void afterSuite() {
    // System.out.println("START AdminTestBase.afterSuite() at " + new
    // Date(System.currentTimeMillis()));
    // stop admin
    _adminThread.stop();

    // stop zk
    ZKClientPool.reset();
    _gZkClient.close();

    TestHelper.stopZkServer(_zkServer);
    // System.out.println("END AdminTestBase.afterSuite() at " + new
    // Date(System.currentTimeMillis()));
  }

}
