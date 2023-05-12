package org.apache.helix.metaclient.impl.zk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


public abstract class ZkMetaClientTestBase {

  protected static final String ZK_ADDR = "localhost:2183";
  protected static final int DEFAULT_TIMEOUT_MS = 1000;
  protected static final String ENTRY_STRING_VALUE = "test-value";
  private static ZkServer _zkServer;

  /**
   * Creates local Zk Server
   * Note: Cannot test container / TTL node end to end behavior as
   * the zk server setup doesn't allow for that. To enable this, zk server
   * setup must invoke ContainerManager.java. However, the actual
   * behavior has been verified to work on native ZK Client.
   * TODO: Modify zk server setup to include ContainerManager.
   * This can be done through ZooKeeperServerMain.java or
   * LeaderZooKeeperServer.java.
   */
  @BeforeSuite
  public void prepare() {
    // Enable extended types and create a ZkClient
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    // start local zookeeper server
    _zkServer = startZkServer(ZK_ADDR);
  }

  @AfterSuite
  public void cleanUp() {
    _zkServer.shutdown();
  }

  protected static ZkMetaClient<String> createZkMetaClient() {
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
            //.setZkSerializer(new TestStringSerializer())
            .build();
    return new ZkMetaClient<>(config);
  }

  public static ZkServer startZkServer(final String zkAddress) {
    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";

    // Clean up local directory
    try {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    } catch (IOException e) {
      e.printStackTrace();
    }

    IDefaultNameSpace defaultNameSpace = zkClient -> {
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    System.out.println("Starting ZK server at " + zkAddress);
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    return zkServer;
  }
}
