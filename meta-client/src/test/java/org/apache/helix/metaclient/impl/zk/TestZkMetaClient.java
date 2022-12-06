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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkMetaClient {

  private static final String ZK_ADDR = "localhost:2183";
  private final Object _syncObject = new Object();
  private ZkServer _zkServer;

  @BeforeClass
  public void prepare() {
    // start local zookeeper server
    _zkServer = startZkServer(ZK_ADDR);
  }

  @Test
  public void testConnect() {
    try (ZkMetaClient zkMetaClient = createZkMetaClient()) {
      boolean connected = zkMetaClient.connect();
      Assert.assertTrue(connected);
    }
  }

  @Test(dependsOnMethods = "testConnect")
  public void testDataChangeListenerTrigger() throws Exception {
    final String path = "/TestZkMetaClient/testDataChangeListener";
    try (ZkMetaClient zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(path, "test-node");
      MockDataChangeListener listener = new MockDataChangeListener();
      zkMetaClient.subscribeDataChange(path, listener, false, true);
      Assert.assertEquals(zkMetaClient.getDataChangeListenerMap().size(), 1);
      Assert.assertEquals(listener.getTriggeredCount(), 0);
      synchronized (_syncObject) {
        zkMetaClient.process(
            new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
        while (listener.getTriggeredCount() == 0) {
          _syncObject.wait(3000);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 1);
      }
      // register another listener
      MockDataChangeListener listener2 = new MockDataChangeListener();
      zkMetaClient.subscribeDataChange(path, listener2, true, true);
      synchronized (_syncObject) {
        zkMetaClient.process(
            new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
        while (listener.getTriggeredCount() == 1) {
          _syncObject.wait(3000);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 2);
        Assert.assertEquals(listener2.getTriggeredCount(), 1);
      }
      // unregister the first listener, it shouldn't be triggered anymore
      zkMetaClient.unsubscribeDataChange(path, listener);
      synchronized (_syncObject) {
        zkMetaClient.process(
            new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
        while (listener2.getTriggeredCount() == 1) {
          _syncObject.wait(3000);
        }
        Assert.assertEquals(listener.getTriggeredCount(), 2);
        Assert.assertEquals(listener2.getTriggeredCount(), 2);
      }
      // TODO register the 3rd listener on a different path
      // remove all listener and expect no more triggering
      zkMetaClient.unsubscribeDataChange(path, listener2);
      Assert.assertTrue(zkMetaClient.getDataChangeListenerMap().isEmpty());
      synchronized (_syncObject) {
        zkMetaClient.process(
            new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
        _syncObject.wait(3000);
        Assert.assertEquals(listener.getTriggeredCount(), 2);
        Assert.assertEquals(listener2.getTriggeredCount(), 2);
      }
    }
  }

  private static ZkMetaClient createZkMetaClient() {
    ZkMetaClientConfig config = new ZkMetaClientConfig.ZkMetaClientConfigBuilder()
        .setZkSerializer(new ZNRecordSerializer())
        .setConnectionAddress(ZK_ADDR)
        .build();
    return new ZkMetaClient(config);
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
    @Override
    public void handleDataChange(String key, Object data, ChangeType changeType) {
      _triggeredCount.getAndIncrement();
      synchronized (_syncObject) {
        _syncObject.notifyAll();
      }
    }

    int getTriggeredCount() {
      return _triggeredCount.get();
    }
  }
}
