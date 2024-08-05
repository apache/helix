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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;


public class TestUtil {

  public static final long AUTO_RECONNECT_TIMEOUT_MS_FOR_TEST = 3 * 1000;
  public static final long AUTO_RECONNECT_WAIT_TIME_WITHIN = 1 * 1000;
  public static final long AUTO_RECONNECT_WAIT_TIME_EXD = 5 * 1000;

  static java.lang.reflect.Field getField(Class clazz, String fieldName)
      throws NoSuchFieldException {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      Class superClass = clazz.getSuperclass();
      if (superClass == null) {
        throw e;
      }
      return getField(superClass, fieldName);
    }
  }

  public static Map<String, List<String>> getZkWatch(RealmAwareZkClient client)
      throws Exception {
    Map<String, List<String>> lists = new HashMap<String, List<String>>();
    ZkClient zkClient = (ZkClient) client;

    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper zk = connection.getZookeeper();

    java.lang.reflect.Field field = getField(zk.getClass(), "watchManager");
    field.setAccessible(true);
    Object watchManager = field.get(zk);

    java.lang.reflect.Field field2 = getField(watchManager.getClass(), "dataWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> dataWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "existWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> existWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "childWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> childWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "persistentWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> persistentWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "persistentRecursiveWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> persistentRecursiveWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);


    lists.put("dataWatches", new ArrayList<>(dataWatches.keySet()));
    lists.put("existWatches", new ArrayList<>(existWatches.keySet()));
    lists.put("childWatches", new ArrayList<>(childWatches.keySet()));
    lists.put("persistentWatches", new ArrayList<>(persistentWatches.keySet()));
    lists.put("persistentRecursiveWatches", new ArrayList<>(persistentRecursiveWatches.keySet()));

    return lists;
  }

  public static void expireSession(ZkMetaClient client)
      throws Exception {
    final CountDownLatch waitNewSession = new CountDownLatch(1);
    final ZkClient zkClient = client.getZkClient();

    IZkStateListener listener = new IZkStateListener() {
      @Override
      public void handleStateChanged(Watcher.Event.KeeperState state)
          throws Exception {
      }

      @Override
      public void handleNewSession(final String sessionId)
          throws Exception {
        // make sure zkclient is connected again
        zkClient.waitUntilConnected(HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS);

        waitNewSession.countDown();
      }

      @Override
      public void handleSessionEstablishmentError(Throwable var1)
          throws Exception {
      }
    };

    zkClient.subscribeStateChanges(listener);

    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper curZookeeper = connection.getZookeeper();
    String oldSessionId = Long.toHexString(curZookeeper.getSessionId());

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
      }
    };

    final ZooKeeper dupZookeeper =
        new ZooKeeper(connection.getServers(), curZookeeper.getSessionTimeout(), watcher,
            curZookeeper.getSessionId(), curZookeeper.getSessionPasswd());
    // wait until connected, then close
    while (dupZookeeper.getState() != ZooKeeper.States.CONNECTED) {
      Thread.sleep(10);
    }
    Assert.assertEquals(dupZookeeper.getState(), ZooKeeper.States.CONNECTED,
        "Fail to connect to zk using current session info");
    dupZookeeper.close();

    // make sure session expiry really happens
    waitNewSession.await();
    zkClient.unsubscribeStateChanges(listener);

    connection = (ZkConnection) zkClient.getConnection();
    curZookeeper = connection.getZookeeper();

    String newSessionId = Long.toHexString(curZookeeper.getSessionId());
    Assert.assertFalse(newSessionId.equals(oldSessionId),
        "Fail to expire current session, zk: " + curZookeeper);
  }



  /**
   * Simulate a zk state change by calling {@link ZkClient#process(WatchedEvent)} directly
   * This need to be done in a separate thread to simulate ZkClient eventThread.
   */
  public static void simulateZkStateReconnected(ZkMetaClient client) throws InterruptedException {
    final ZkClient zkClient = client.getZkClient();
    WatchedEvent event =
        new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected,
            null);
    zkClient.process(event);

    Thread.sleep(AUTO_RECONNECT_WAIT_TIME_WITHIN);

    event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected,
        null);
    zkClient.process(event);
  }

  /**
   * Simulate a zk state change by calling {@link ZkClient#process(WatchedEvent)} directly
   * This need to be done in a separate thread to simulate ZkClient eventThread.
   */
  public static void simulateZkStateClosedAndReconnect(ZkMetaClient client) throws InterruptedException {
    final ZkClient zkClient = client.getZkClient();
    WatchedEvent event =
        new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Closed,
            null);
    zkClient.process(event);

    Thread.sleep(AUTO_RECONNECT_WAIT_TIME_WITHIN);

    event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected,
        null);
    zkClient.process(event);
  }

  /**
   * return the name of the calling test method
   */
  public static String getTestMethodName() {
    StackTraceElement[] calls = Thread.currentThread().getStackTrace();
    return calls[2].getMethodName();
  }

}