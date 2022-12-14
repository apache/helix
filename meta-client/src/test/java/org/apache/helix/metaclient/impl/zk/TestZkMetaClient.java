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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkMetaClient {

  private static final String ZK_ADDR = "localhost:2183";
  private static final int DEFAULT_TIMEOUT = 1000;
  private final Object _syncObject = new Object();
  private ZkServer _zkServer;

  @BeforeClass
  public void prepare() {
    // start local zookeeper server
    _zkServer = startZkServer(ZK_ADDR);
  }

  @Test
  public void testConnect() {
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      boolean connected = zkMetaClient.connect();
      Assert.assertTrue(connected);
    }
  }

  @Test(dependsOnMethods = "testConnect")
  public void testDataChangeListenerTriggerWithZkWatcher() throws InterruptedException {
    final String path = "/TestZkMetaClient/testTriggerWithZkWatcher";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      MockDataChangeListener listener = new MockDataChangeListener();
      zkMetaClient.subscribeDataChange(path, listener, false, true);
      zkMetaClient.create(path, "test-node");
      synchronized (_syncObject) {
        while (listener.getTriggeredCount() == 0) {
          _syncObject.wait(DEFAULT_TIMEOUT);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 1);
        Assert.assertEquals(listener.getLastEventType(), DataChangeListener.ChangeType.ENTRY_CREATED);
      }
      zkMetaClient.set(path, "test-node-changed", -1);
      synchronized (_syncObject) {
        while (listener.getTriggeredCount() == 1) {
          _syncObject.wait(DEFAULT_TIMEOUT);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 2);
        Assert.assertEquals(listener.getLastEventType(), DataChangeListener.ChangeType.ENTRY_UPDATE);
      }
      zkMetaClient.delete(path);
      synchronized (_syncObject) {
        while (listener.getTriggeredCount() == 2) {
          _syncObject.wait(DEFAULT_TIMEOUT);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 3);
        Assert.assertEquals(listener.getLastEventType(), DataChangeListener.ChangeType.ENTRY_DELETED);
      }
      // unregister listener, expect no more call
      zkMetaClient.unsubscribeDataChange(path, listener);
      // register a new non-persistent listener
      MockDataChangeListener listener2 = new MockDataChangeListener();
      zkMetaClient.subscribeDataChange(path, listener2, false, false);
      zkMetaClient.create(path, "test-node");
      synchronized (_syncObject) {
        while (listener2.getTriggeredCount() == 0) {
          _syncObject.wait(DEFAULT_TIMEOUT);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 3);
        Assert.assertEquals(listener2.getTriggeredCount(), 1);
        Assert.assertEquals(listener2.getLastEventType(), DataChangeListener.ChangeType.ENTRY_CREATED);
      }
      // neither listener should be triggered
      zkMetaClient.delete(path);
      synchronized (_syncObject) {
        _syncObject.wait(DEFAULT_TIMEOUT);
        Assert.assertEquals(listener.getTriggeredCount(), 3);
        Assert.assertEquals(listener2.getTriggeredCount(), 1);
      }
    }
  }

  @Test(dependsOnMethods = "testDataChangeListenerTriggerWithZkWatcher")
  public void testMultipleDataChangeListeners() throws Exception {
    final String basePath = "/TestZkMetaClient/testMultipleDataChangeListeners";
    final int count = 5;
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      Map<String, Set<DataChangeListener>> listeners = new HashMap<>();
      CountDownLatch countDownLatch = new CountDownLatch(count);
      // create paths
      for (int i = 0; i < 2; i++) {
        String path = basePath + "/" + i;
        listeners.put(path, new HashSet<>());
        // 5 listeners for each path
        for (int j = 0; j < count; j++) {
          DataChangeListener listener = new DataChangeListener() {
            @Override
            public void handleDataChange(String key, Object data, ChangeType changeType) throws Exception {
              countDownLatch.countDown();
            }
          };
          listeners.get(path).add(listener);
          zkMetaClient.subscribeDataChange(path, listener, false, true);
        }
      }
      zkMetaClient.create(basePath + "/1", "test-data");
      Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));
    }
  }

  private static ZkMetaClient<String> createZkMetaClient() {
    ZkMetaClientConfig config = new ZkMetaClientConfig.ZkMetaClientConfigBuilder()
        .setConnectionAddress(ZK_ADDR)
        .build();
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
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    System.out.println("Starting ZK server at " + zkAddress);
    zkServer.start();
    return zkServer;
  }

  private class MockDataChangeListener implements DataChangeListener {
    private final AtomicInteger _triggeredCount = new AtomicInteger(0);
    private volatile ChangeType _lastEventType;

    @Override
    public void handleDataChange(String key, Object data, ChangeType changeType) {
      _triggeredCount.getAndIncrement();
      _lastEventType = changeType;
      synchronized (_syncObject) {
        _syncObject.notifyAll();
      }
    }

    int getTriggeredCount() {
      return _triggeredCount.get();
    }

    ChangeType getLastEventType() {
      return _lastEventType;
    }
  }
}
