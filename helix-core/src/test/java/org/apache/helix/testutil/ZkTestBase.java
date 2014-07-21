package org.apache.helix.testutil;

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

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class ZkTestBase {
  static Logger logger = Logger.getLogger(ZkTestBase.class);
  static ZkServer zkserver;

  // Used by tests
  protected static ZkClient _zkclient;
  protected static String _zkaddr;
  protected static BaseDataAccessor<ZNRecord> _baseAccessor;
  protected static ClusterSetup _setupTool;

  public static synchronized void startZkServerIfNot() {
    if (zkserver == null) {
      zkserver = ZkTestUtil.startZkServer();
      _zkaddr = "localhost:" + zkserver.getPort();
      _zkclient =
          new ZkClient(_zkaddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

      _baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
      _setupTool = new ClusterSetup(_zkclient);

      ZKClientPool.reset();
    }
  }

  public static synchronized void stopZkServer() {
    if (zkserver != null) {
      _baseAccessor = null;
      _zkclient.close();
      _zkclient = null;
      _setupTool = null;
      _zkaddr = null;
      zkserver.shutdown();
      zkserver = null;
    }
  }

  @BeforeSuite
  public void beforeSuite() {
    startZkServerIfNot();
  }

  @AfterSuite
  public void afterSuite() {
    stopZkServer();
  }
}
