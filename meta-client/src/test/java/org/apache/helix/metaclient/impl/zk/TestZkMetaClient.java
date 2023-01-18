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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.helix.metaclient.api.DataUpdater;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.constants.MetaClientException;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.metaclient.api.MetaClientInterface.EntryMode.PERSISTENT;


public class TestZkMetaClient {

  private static final String ZK_ADDR = "localhost:2183";
  private ZkServer _zkServer;
  private static final String ENTRY_STRING_VALUE = "test-value";

  @BeforeClass
  public void prepare() {
    // start local zookeeper server
    _zkServer = startZkServer(ZK_ADDR);
  }

  @AfterClass
  public void cleanUp() {
    _zkServer.shutdown();
  }

  @Test
  public void testCreate() {
    final String key = "/TestZkMetaClient_testCreate";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      Assert.assertNotNull(zkMetaClient.exists(key));

      try {
        zkMetaClient.create("a/b/c", "invalid_path");
        Assert.fail("Should have failed with incorrect path.");
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testGet() {
    final String key = "/TestZkMetaClient_testGet";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      String value;
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      String dataValue = zkMetaClient.get(key);
      Assert.assertEquals(dataValue, ENTRY_STRING_VALUE);

      value = zkMetaClient.get(key + "/a/b/c");
      Assert.assertNull(value);

      zkMetaClient.delete(key);

      value = zkMetaClient.get(key);
      Assert.assertNull(value);
    }
  }

  @Test
  public void testSet() {
    final String key = "/TestZkMetaClient_testSet";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      String testValueV1 = ENTRY_STRING_VALUE + "-v1";
      String testValueV2 = ENTRY_STRING_VALUE + "-v2";

      // test set() with no expected version and validate result.
      zkMetaClient.set(key, testValueV1, -1);
      Assert.assertEquals(zkMetaClient.get(key), testValueV1);
      MetaClientInterface.Stat entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 1);
      Assert.assertEquals(entryStat.getEntryType().name(), PERSISTENT.name());

      // test set() with expected version and validate result and new version number
      zkMetaClient.set(key, testValueV2, 1);
      entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(zkMetaClient.get(key), testValueV2);
      Assert.assertEquals(entryStat.getVersion(), 2);

      // test set() with a wrong version
      try {
        zkMetaClient.set(key, "test-node-changed", 10);
        Assert.fail("No reach.");
      } catch (MetaClientException ex) {
        Assert.assertEquals(ex.getClass().getName(),
            "org.apache.helix.metaclient.constants.MetaClientBadVersionException");
      }
      zkMetaClient.delete(key);
    }
  }

  @Test
  public void testUpdate() {
    final String key = "/TestZkMetaClient_testUpdate";
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR).build();
    try (ZkMetaClient<Integer> zkMetaClient = new ZkMetaClient<>(config)) {
      zkMetaClient.connect();
      int initValue = 3;
      zkMetaClient.create(key, initValue);
      MetaClientInterface.Stat entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 0);

      // test update() and validate entry value and version
      Integer newData = zkMetaClient.update(key, new DataUpdater<Integer>() {
        @Override
        public Integer update(Integer currentData) {
          return currentData + 1;
        }
      });
      Assert.assertEquals((int) newData, (int) initValue + 1);

      entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 1);

      newData = zkMetaClient.update(key, new DataUpdater<Integer>() {

        @Override
        public Integer update(Integer currentData) {
          return currentData + 1;
        }
      });

      entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 2);
      Assert.assertEquals((int) newData, (int) initValue + 2);
      zkMetaClient.delete(key);
    }
  }

  @Test
  public void testGetAndCountChildrenAndRecursiveDelete() {
    final String key = "/TestZkMetaClient_testGetAndCountChildren";
    List<String> childrenNames = Arrays.asList("/c1", "/c2", "/c3");

    // create child nodes and validate retrieved children count and names
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      Assert.assertEquals(zkMetaClient.countDirectChildren(key), 0);
      for (String str : childrenNames) {
        zkMetaClient.create(key + str, ENTRY_STRING_VALUE);
      }

      List<String> retrievedChildrenNames = zkMetaClient.getDirectChildrenKeys(key);
      Assert.assertEquals(retrievedChildrenNames.size(), childrenNames.size());
      Set<String> childrenNameSet = new HashSet<>(childrenNames);
      for (String str : retrievedChildrenNames) {
        Assert.assertTrue(childrenNameSet.contains("/" + str));
      }

      // recursive delete and validate
      Assert.assertEquals(zkMetaClient.countDirectChildren(key), childrenNames.size());
      Assert.assertNotNull(zkMetaClient.exists(key));
      zkMetaClient.recursiveDelete(key);
      Assert.assertNull(zkMetaClient.exists(key));
    }
  }



  private static ZkMetaClient<String> createZkMetaClient() {
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR).build();
    return new ZkMetaClient<>(config);
  }

  private static ZkServer startZkServer(final String zkAddress) {
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
