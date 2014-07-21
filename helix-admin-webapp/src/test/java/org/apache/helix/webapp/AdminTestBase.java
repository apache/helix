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

import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.webapp.AdminTestHelper.AdminThread;
import org.apache.log4j.Logger;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class AdminTestBase extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(AdminTestBase.class);
  protected final static int ADMIN_PORT = 2202;

  protected static Client _gClient;

  static AdminThread _adminThread;

  @BeforeSuite
  public void beforeSuite() {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    // start zk
    super.beforeSuite();

    // start admin
    _adminThread = new AdminThread(_zkaddr, ADMIN_PORT);
    _adminThread.start();

    // create a client
    _gClient = new Client(Protocol.HTTP);

    // wait for the web service to start
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      LOG.error("Interrupted", e);
    }
  }

  @AfterSuite
  public void afterSuite() {
    // System.out.println("START AdminTestBase.afterSuite() at " + new
    // Date(System.currentTimeMillis()));
    // stop admin
    _adminThread.stop();

    // stop zk
    super.afterSuite();
    // System.out.println("END AdminTestBase.afterSuite() at " + new
    // Date(System.currentTimeMillis()));
  }

}
