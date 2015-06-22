package org.apache.helix;

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.testng.Assert;

public class ZkTestHelper {
  private static Logger LOG = Logger.getLogger(ZkTestHelper.class);

  static {
    // Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  public static void disconnectSession(final ZkClient zkClient) throws Exception {
    IZkStateListener listener = new IZkStateListener() {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception {
        // System.err.println("disconnectSession handleStateChanged. state: " + state);
      }

      @Override
      public void handleNewSession() throws Exception {
        // make sure zkclient is connected again
        zkClient.waitUntilConnected();

        ZkConnection connection = ((ZkConnection) zkClient.getConnection());
        ZooKeeper curZookeeper = connection.getZookeeper();

        LOG.info("handleNewSession. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));
      }
    };

    zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper curZookeeper = connection.getZookeeper();
    LOG.info("Before expiry. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("Process watchEvent: " + event);
      }
    };

    final ZooKeeper dupZookeeper =
        new ZooKeeper(connection.getServers(), curZookeeper.getSessionTimeout(), watcher,
            curZookeeper.getSessionId(), curZookeeper.getSessionPasswd());
    // wait until connected, then close
    while (dupZookeeper.getState() != States.CONNECTED) {
      Thread.sleep(10);
    }
    dupZookeeper.close();

    connection = (ZkConnection) zkClient.getConnection();
    curZookeeper = connection.getZookeeper();
    zkClient.unsubscribeStateChanges(listener);

    // System.err.println("zk: " + oldZookeeper);
    LOG.info("After expiry. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));
  }

  /**
   * Simulate a zk state change by calling {@link ZkClient#process(WatchedEvent)} directly
   */
  public static void simulateZkStateDisconnected(ZkClient client) {
    WatchedEvent event = new WatchedEvent(EventType.None, KeeperState.Disconnected, null);
    client.process(event);
  }

  /**
   * Get zk connection session id
   * @param client
   * @return
   */
  public static String getSessionId(ZkClient client) {
    ZkConnection connection = ((ZkConnection) client.getConnection());
    ZooKeeper curZookeeper = connection.getZookeeper();
    return Long.toHexString(curZookeeper.getSessionId());
  }

  /**
   * Expire current zk session and wait for {@link IZkStateListener#handleNewSession()} invoked
   * @param zkClient
   * @throws Exception
   */
  public static void expireSession(final ZkClient zkClient) throws Exception {
    final CountDownLatch waitNewSession = new CountDownLatch(1);

    IZkStateListener listener = new IZkStateListener() {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception {
        LOG.info("IZkStateListener#handleStateChanged, state: " + state);
      }

      @Override
      public void handleNewSession() throws Exception {
        // make sure zkclient is connected again
        zkClient.waitUntilConnected();

        ZkConnection connection = ((ZkConnection) zkClient.getConnection());
        ZooKeeper curZookeeper = connection.getZookeeper();

        LOG.info("handleNewSession. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));
        waitNewSession.countDown();
      }
    };

    zkClient.subscribeStateChanges(listener);

    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper curZookeeper = connection.getZookeeper();
    String oldSessionId = Long.toHexString(curZookeeper.getSessionId());
    LOG.info("Before session expiry. sessionId: " + oldSessionId + ", zk: " + curZookeeper);

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("Watcher#process, event: " + event);
      }
    };

    final ZooKeeper dupZookeeper =
        new ZooKeeper(connection.getServers(), curZookeeper.getSessionTimeout(), watcher,
            curZookeeper.getSessionId(), curZookeeper.getSessionPasswd());
    // wait until connected, then close
    while (dupZookeeper.getState() != States.CONNECTED) {
      Thread.sleep(10);
    }
    Assert.assertEquals(dupZookeeper.getState(), States.CONNECTED,
        "Fail to connect to zk using current session info");
    dupZookeeper.close();

    // make sure session expiry really happens
    waitNewSession.await();
    zkClient.unsubscribeStateChanges(listener);

    connection = (ZkConnection) zkClient.getConnection();
    curZookeeper = connection.getZookeeper();

    String newSessionId = Long.toHexString(curZookeeper.getSessionId());
    LOG.info("After session expiry. sessionId: " + newSessionId + ", zk: " + curZookeeper);
    Assert.assertNotSame(newSessionId, oldSessionId, "Fail to expire current session, zk: "
        + curZookeeper);
  }

  /**
   * expire zk session asynchronously
   * @param zkClient
   * @throws Exception
   */
  public static void asyncExpireSession(final ZkClient zkClient) throws Exception {
    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper curZookeeper = connection.getZookeeper();
    LOG.info("Before expiry. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("Process watchEvent: " + event);
      }
    };

    final ZooKeeper dupZookeeper =
        new ZooKeeper(connection.getServers(), curZookeeper.getSessionTimeout(), watcher,
            curZookeeper.getSessionId(), curZookeeper.getSessionPasswd());
    // wait until connected, then close
    while (dupZookeeper.getState() != States.CONNECTED) {
      Thread.sleep(10);
    }
    dupZookeeper.close();

    connection = (ZkConnection) zkClient.getConnection();
    curZookeeper = connection.getZookeeper();

    // System.err.println("zk: " + oldZookeeper);
    LOG.info("After expiry. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));
  }

  /*
   * stateMap: partition->instance->state
   */
  public static boolean verifyState(ZkClient zkclient, String clusterName, String resourceName,
      Map<String, Map<String, String>> expectStateMap, String op) {
    boolean result = true;
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(zkclient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    ExternalView extView = accessor.getProperty(keyBuilder.externalView(resourceName));
    Map<String, Map<String, String>> actualStateMap = extView.getRecord().getMapFields();
    for (String partition : actualStateMap.keySet()) {
      for (String expectPartiton : expectStateMap.keySet()) {
        if (!partition.matches(expectPartiton)) {
          continue;
        }

        Map<String, String> actualInstanceStateMap = actualStateMap.get(partition);
        Map<String, String> expectInstanceStateMap = expectStateMap.get(expectPartiton);
        for (String instance : actualInstanceStateMap.keySet()) {
          for (String expectInstance : expectStateMap.get(expectPartiton).keySet()) {
            if (!instance.matches(expectInstance)) {
              continue;
            }

            String actualState = actualInstanceStateMap.get(instance);
            String expectState = expectInstanceStateMap.get(expectInstance);
            boolean equals = expectState.equals(actualState);
            if (op.equals("==") && !equals || op.equals("!=") && equals) {
              System.out.println(partition + "/" + instance + " state mismatch. actual state: "
                  + actualState + ", but expect: " + expectState + ", op: " + op);
              result = false;
            }
          }
        }
      }
    }
    return result;
  }

  /**
   * return the number of listeners on given zk-path
   * @param zkAddr
   * @param path
   * @return
   * @throws Exception
   */
  public static int numberOfListeners(String zkAddr, String path) throws Exception {
    Map<String, Set<String>> listenerMap = getListenersByZkPath(zkAddr);
    if (listenerMap.containsKey(path)) {
      return listenerMap.get(path).size();
    }
    return 0;
  }

  /**
   * return a map from zk-path to a set of zk-session-id that put watches on the zk-path
   * @param zkAddr
   * @return
   * @throws Exception
   */
  public static Map<String, Set<String>> getListenersByZkPath(String zkAddr) throws Exception {
    String splits[] = zkAddr.split(":");
    Map<String, Set<String>> listenerMap = new TreeMap<String, Set<String>>();
    Socket sock = null;
    int retry = 5;

    while (retry > 0) {
      try {
        sock = new Socket(splits[0], Integer.parseInt(splits[1]));
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));

        out.println("wchp");

        listenerMap.clear();
        String lastPath = null;
        String line = in.readLine();
        while (line != null) {
          line = line.trim();

          if (line.startsWith("/")) {
            lastPath = line;
            if (!listenerMap.containsKey(lastPath)) {
              listenerMap.put(lastPath, new TreeSet<String>());
            }
          } else if (line.startsWith("0x")) {
            if (lastPath != null && listenerMap.containsKey(lastPath)) {
              listenerMap.get(lastPath).add(line);
            } else {
              LOG.error("Not path associated with listener sessionId: " + line + ", lastPath: "
                  + lastPath);
            }
          } else {
            // LOG.error("unrecognized line: " + line);
          }
          line = in.readLine();
        }
        break;
      } catch (Exception e) {
        // sometimes in test, we see connection-reset exceptions when in.readLine()
        // so add this retry logic
        retry--;
      } finally {
        if (sock != null)
          sock.close();
      }
    }
    return listenerMap;
  }

  /**
   * return a map from session-id to a set of zk-path that the session has watches on
   * @return
   */
  public static Map<String, Set<String>> getListenersBySession(String zkAddr) throws Exception {
    Map<String, Set<String>> listenerMapByInstance = getListenersByZkPath(zkAddr);

    // convert to index by sessionId
    Map<String, Set<String>> listenerMapBySession = new TreeMap<String, Set<String>>();
    for (String path : listenerMapByInstance.keySet()) {
      for (String sessionId : listenerMapByInstance.get(path)) {
        if (!listenerMapBySession.containsKey(sessionId)) {
          listenerMapBySession.put(sessionId, new TreeSet<String>());
        }
        listenerMapBySession.get(sessionId).add(path);
      }
    }

    return listenerMapBySession;
  }

  static java.lang.reflect.Field getField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      Class<?> superClass = clazz.getSuperclass();
      if (superClass == null) {
        throw e;
      } else {
        return getField(superClass, fieldName);
      }
    }
  }

  public static Map<String, List<String>> getZkWatch(ZkClient client) throws Exception {
    Map<String, List<String>> lists = new HashMap<String, List<String>>();
    ZkConnection connection = ((ZkConnection) client.getConnection());
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

    lists.put("dataWatches", new ArrayList<String>(dataWatches.keySet()));
    lists.put("existWatches", new ArrayList<String>(existWatches.keySet()));
    lists.put("childWatches", new ArrayList<String>(childWatches.keySet()));

    return lists;
  }

  public static Map<String, Set<IZkDataListener>> getZkDataListener(ZkClient client)
      throws Exception {
    java.lang.reflect.Field field = getField(client.getClass(), "_dataListener");
    field.setAccessible(true);
    Map<String, Set<IZkDataListener>> dataListener =
        (Map<String, Set<IZkDataListener>>) field.get(client);
    return dataListener;
  }

  public static Map<String, Set<IZkChildListener>> getZkChildListener(ZkClient client)
      throws Exception {
    java.lang.reflect.Field field = getField(client.getClass(), "_childListener");
    field.setAccessible(true);
    Map<String, Set<IZkChildListener>> childListener =
        (Map<String, Set<IZkChildListener>>) field.get(client);
    return childListener;
  }

  public static boolean tryWaitZkEventsCleaned(ZkClient zkclient) throws Exception {
    java.lang.reflect.Field field = getField(zkclient.getClass(), "_eventThread");
    field.setAccessible(true);
    Object eventThread = field.get(zkclient);
    // System.out.println("field: " + eventThread);

    java.lang.reflect.Field field2 = getField(eventThread.getClass(), "_events");
    field2.setAccessible(true);
    BlockingQueue queue = (BlockingQueue) field2.get(eventThread);
    // System.out.println("field2: " + queue + ", " + queue.size());

    if (queue == null) {
      LOG.error("fail to get event-queue from zkclient. skip waiting");
      return false;
    }

    for (int i = 0; i < 20; i++) {
      if (queue.size() == 0) {
        return true;
      }
      Thread.sleep(100);
      System.out.println("pending zk-events in queue: " + queue);
    }
    return false;
  }
}
