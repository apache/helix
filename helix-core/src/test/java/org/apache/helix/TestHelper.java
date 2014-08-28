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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.State;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkCallbackHandler;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.StateModelDefinition.StateModelDefinitionProperty;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;

public class TestHelper {
  private static final Logger LOG = Logger.getLogger(TestHelper.class);

  static public ZkServer startZkServer(final String zkAddress) throws Exception {
    List<String> empty = Collections.emptyList();
    return TestHelper.startZkServer(zkAddress, empty, true);
  }

  static public ZkServer startZkServer(final String zkAddress, final String rootNamespace)
      throws Exception {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkServer(zkAddress, rootNamespaces, true);
  }

  static public ZkServer startZkServer(final String zkAddress, final List<String> rootNamespaces)
      throws Exception {
    return startZkServer(zkAddress, rootNamespaces, true);
  }

  static public ZkServer startZkServer(final String zkAddress, final List<String> rootNamespaces,
      boolean overwrite) throws Exception {
    System.out.println("Start zookeeper at " + zkAddress + " in thread "
        + Thread.currentThread().getName());

    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    if (overwrite) {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    }
    ZKClientPool.reset();

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        if (rootNamespaces == null) {
          return;
        }

        for (String rootNamespace : rootNamespaces) {
          try {
            zkClient.deleteRecursive(rootNamespace);
          } catch (Exception e) {
            LOG.error("fail to deleteRecursive path:" + rootNamespace, e);
          }
        }
      }
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();

    return zkServer;
  }

  static public void stopZkServer(ZkServer zkServer) {
    if (zkServer != null) {
      zkServer.shutdown();
      System.out.println("Shut down zookeeper at port " + zkServer.getPort() + " in thread "
          + Thread.currentThread().getName());
    }
  }

  public static void setupEmptyCluster(ZkClient zkClient, String clusterName) {
    ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
    admin.addCluster(clusterName, true);
  }

  /**
   * convert T[] to set<T>
   * @param s
   * @return
   */
  public static <T> Set<T> setOf(T... s) {
    Set<T> set = new HashSet<T>(Arrays.asList(s));
    return set;
  }

  /**
   * generic method for verification with a timeout
   * @param verifierName
   * @param args
   */
  public static void verifyWithTimeout(String verifierName, long timeout, Object... args) {
    final long sleepInterval = 1000; // in ms
    final int loop = (int) (timeout / sleepInterval) + 1;
    try {
      boolean result = false;
      int i = 0;
      for (; i < loop; i++) {
        Thread.sleep(sleepInterval);
        // verifier should be static method
        result = (Boolean) TestHelper.getMethod(verifierName).invoke(null, args);

        if (result == true) {
          break;
        }
      }

      // debug
      // LOG.info(verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify ("
      // + result + ")");
      System.err.println(verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify " + " ("
          + result + ")");
      LOG.debug("args:" + Arrays.toString(args));
      // System.err.println("args:" + Arrays.toString(args));

      if (result == false) {
        LOG.error(verifierName + " fails");
        LOG.error("args:" + Arrays.toString(args));
      }

      Assert.assertTrue(result);
    } catch (Exception e) {
      LOG.error("Exception in verify: " + verifierName, e);
    }
  }

  private static Method getMethod(String name) {
    Method[] methods = TestHelper.class.getMethods();
    for (Method method : methods) {
      if (name.equals(method.getName())) {
        return method;
      }
    }
    return null;
  }

  public static boolean verifyEmptyCurStateAndExtView(String clusterName, String resourceName,
      Set<String> instanceNames, String zkAddr) {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try {
      ZKHelixDataAccessor accessor =
          new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
      Builder keyBuilder = accessor.keyBuilder();

      for (String instanceName : instanceNames) {
        List<String> sessionIds = accessor.getChildNames(keyBuilder.sessions(instanceName));

        for (String sessionId : sessionIds) {
          CurrentState curState =
              accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, resourceName));

          if (curState != null && curState.getRecord().getMapFields().size() != 0) {
            return false;
          }
        }

        ExternalView extView = accessor.getProperty(keyBuilder.externalView(resourceName));

        if (extView != null && extView.getRecord().getMapFields().size() != 0) {
          return false;
        }

      }

      return true;
    } finally {
      zkClient.close();
    }
  }

  public static boolean verifyNotConnected(HelixManager manager) {
    return !manager.isConnected();
  }

  public static void setupCluster(String clusterName, String zkAddr, int startPort,
      String participantNamePrefix, String resourceNamePrefix, int resourceNb, int partitionNb,
      int nodesNb, int replica, String stateModelDef, boolean doRebalance) throws Exception {
    TestHelper.setupCluster(clusterName, zkAddr, startPort, participantNamePrefix,
        resourceNamePrefix, resourceNb, partitionNb, nodesNb, replica, stateModelDef,
        RebalanceMode.SEMI_AUTO, doRebalance);
  }

  public static void setupCluster(String clusterName, String ZkAddr, int startPort,
      String participantNamePrefix, String resourceNamePrefix, int resourceNb, int partitionNb,
      int nodesNb, int replica, String stateModelDef, RebalanceMode mode, boolean doRebalance)
      throws Exception {
    ZkClient zkClient = new ZkClient(ZkAddr);
    if (zkClient.exists("/" + clusterName)) {
      LOG.warn("Cluster already exists:" + clusterName + ". Deleting it");
      zkClient.deleteRecursive("/" + clusterName);
    }

    ClusterSetup setupTool = new ClusterSetup(ZkAddr);
    setupTool.addCluster(clusterName, true);

    for (int i = 0; i < nodesNb; i++) {
      int port = startPort + i;
      setupTool.addInstanceToCluster(clusterName, participantNamePrefix + "_" + port);
    }

    for (int i = 0; i < resourceNb; i++) {
      String resourceName = resourceNamePrefix + i;
      setupTool.addResourceToCluster(clusterName, resourceName, partitionNb, stateModelDef,
          mode.toString());
      if (doRebalance) {
        setupTool.rebalanceStorageCluster(clusterName, resourceName, replica);
      }
    }
    zkClient.close();
  }

  /**
   * @param stateMap
   *          : "ResourceName/partitionKey" -> setOf(instances)
   * @param state
   *          : MASTER|SLAVE|ERROR...
   */
  public static void verifyState(String clusterName, String zkAddr,
      Map<String, Set<String>> stateMap, String state) {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try {
      ZKHelixDataAccessor accessor =
          new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
      Builder keyBuilder = accessor.keyBuilder();

      for (String resGroupPartitionKey : stateMap.keySet()) {
        Map<String, String> retMap = getResourceAndPartitionKey(resGroupPartitionKey);
        String resGroup = retMap.get("RESOURCE");
        String partitionKey = retMap.get("PARTITION");

        ExternalView extView = accessor.getProperty(keyBuilder.externalView(resGroup));
        for (String instance : stateMap.get(resGroupPartitionKey)) {
          String actualState = extView.getStateMap(partitionKey).get(instance);
          Assert.assertNotNull(actualState, "externalView doesn't contain state for " + resGroup
              + "/" + partitionKey + " on " + instance + " (expect " + state + ")");

          Assert
              .assertEquals(actualState, state, "externalView for " + resGroup + "/" + partitionKey
                  + " on " + instance + " is " + actualState + " (expect " + state + ")");
        }
      }
    } finally {
      zkClient.close();
    }
  }

  /**
   * @param resourcePartition
   *          : key is in form of "resource/partitionKey" or "resource_x"
   * @return
   */
  private static Map<String, String> getResourceAndPartitionKey(String resourcePartition) {
    String resourceName;
    String partitionName;
    int idx = resourcePartition.indexOf('/');
    if (idx > -1) {
      resourceName = resourcePartition.substring(0, idx);
      partitionName = resourcePartition.substring(idx + 1);
    } else {
      idx = resourcePartition.lastIndexOf('_');
      resourceName = resourcePartition.substring(0, idx);
      partitionName = resourcePartition;
    }

    Map<String, String> retMap = new HashMap<String, String>();
    retMap.put("RESOURCE", resourceName);
    retMap.put("PARTITION", partitionName);
    return retMap;
  }

  public static <T> Map<String, T> startThreadsConcurrently(final int nrThreads,
      final Callable<T> method, final long timeout) {
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishCounter = new CountDownLatch(nrThreads);
    final Map<String, T> resultsMap = new ConcurrentHashMap<String, T>();
    final List<Thread> threadList = new ArrayList<Thread>();

    for (int i = 0; i < nrThreads; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            boolean isTimeout = !startLatch.await(timeout, TimeUnit.SECONDS);
            if (isTimeout) {
              LOG.error("Timeout while waiting for start latch");
            }
          } catch (InterruptedException ex) {
            LOG.error("Interrupted while waiting for start latch");
          }

          try {
            T result = method.call();
            if (result != null) {
              resultsMap.put("thread_" + this.getId(), result);
            }
            LOG.debug("result=" + result);
          } catch (Exception e) {
            LOG.error("Exeption in executing " + method.getClass().getName(), e);
          }

          finishCounter.countDown();
        }
      };
      threadList.add(thread);
      thread.start();
    }
    startLatch.countDown();

    // wait for all thread to complete
    try {
      boolean isTimeout = !finishCounter.await(timeout, TimeUnit.SECONDS);
      if (isTimeout) {
        LOG.error("Timeout while waiting for finish latch. Interrupt all threads");
        for (Thread thread : threadList) {
          thread.interrupt();
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for finish latch", e);
    }

    return resultsMap;
  }

  public static Message createMessage(MessageId msgId, String fromState, String toState,
      String tgtName, String resourceName, String partitionName) {
    Message msg = new Message(MessageType.STATE_TRANSITION, msgId);
    msg.setFromState(State.from(fromState));
    msg.setToState(State.from(toState));
    msg.setTgtName(tgtName);
    msg.setResourceId(ResourceId.from(resourceName));
    msg.setPartitionId(PartitionId.from(partitionName));
    msg.setStateModelDef(StateModelDefId.from("MasterSlave"));

    return msg;
  }

  public static String getTestMethodName() {
    StackTraceElement[] calls = Thread.currentThread().getStackTrace();
    return calls[2].getMethodName();
  }

  public static String getTestClassName() {
    StackTraceElement[] calls = Thread.currentThread().getStackTrace();
    String fullClassName = calls[2].getClassName();
    return fullClassName.substring(fullClassName.lastIndexOf('.') + 1);
  }

  public static <T> Map<String, T> startThreadsConcurrently(final List<Callable<T>> methods,
      final long timeout) {
    final int nrThreads = methods.size();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishCounter = new CountDownLatch(nrThreads);
    final Map<String, T> resultsMap = new ConcurrentHashMap<String, T>();
    final List<Thread> threadList = new ArrayList<Thread>();

    for (int i = 0; i < nrThreads; i++) {
      final Callable<T> method = methods.get(i);

      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            boolean isTimeout = !startLatch.await(timeout, TimeUnit.SECONDS);
            if (isTimeout) {
              LOG.error("Timeout while waiting for start latch");
            }
          } catch (InterruptedException ex) {
            LOG.error("Interrupted while waiting for start latch");
          }

          try {
            T result = method.call();
            if (result != null) {
              resultsMap.put("thread_" + this.getId(), result);
            }
            LOG.debug("result=" + result);
          } catch (Exception e) {
            LOG.error("Exeption in executing " + method.getClass().getName(), e);
          }

          finishCounter.countDown();
        }
      };
      threadList.add(thread);
      thread.start();
    }
    startLatch.countDown();

    // wait for all thread to complete
    try {
      boolean isTimeout = !finishCounter.await(timeout, TimeUnit.SECONDS);
      if (isTimeout) {
        LOG.error("Timeout while waiting for finish latch. Interrupt all threads");
        for (Thread thread : threadList) {
          thread.interrupt();
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for finish latch", e);
    }

    return resultsMap;
  }

  public static void printCache(Map<String, ZNode> cache) {
    System.out.println("START:Print cache");
    TreeMap<String, ZNode> map = new TreeMap<String, ZNode>();
    map.putAll(cache);

    for (String key : map.keySet()) {
      ZNode node = map.get(key);
      TreeSet<String> childSet = new TreeSet<String>();
      childSet.addAll(node.getChildSet());
      System.out.print(key + "=" + node.getData() + ", " + childSet + ", "
          + (node.getStat() == null ? "null\n" : node.getStat()));
    }
    System.out.println("END:Print cache");
  }

  public static void readZkRecursive(String path, Map<String, ZNode> map, ZkClient zkclient) {
    try {
      Stat stat = new Stat();
      ZNRecord record = zkclient.readData(path, stat);
      List<String> childNames = zkclient.getChildren(path);
      ZNode node = new ZNode(path, record, stat);
      node.addChildren(childNames);
      map.put(path, node);

      for (String childName : childNames) {
        String childPath = path + "/" + childName;
        readZkRecursive(childPath, map, zkclient);
      }
    } catch (ZkNoNodeException e) {
      // OK
    }
  }

  public static void readZkRecursive(String path, Map<String, ZNode> map,
      BaseDataAccessor<ZNRecord> zkAccessor) {
    try {
      Stat stat = new Stat();
      ZNRecord record = zkAccessor.get(path, stat, 0);
      List<String> childNames = zkAccessor.getChildNames(path, 0);
      // System.out.println("childNames: " + childNames);
      ZNode node = new ZNode(path, record, stat);
      node.addChildren(childNames);
      map.put(path, node);

      if (childNames != null && !childNames.isEmpty()) {
        for (String childName : childNames) {
          String childPath = path + "/" + childName;
          readZkRecursive(childPath, map, zkAccessor);
        }
      }
    } catch (ZkNoNodeException e) {
      // OK
    }
  }

  public static boolean verifyZkCache(List<String> paths, BaseDataAccessor<ZNRecord> zkAccessor,
      ZkClient zkclient, boolean needVerifyStat) {
    // read everything
    Map<String, ZNode> zkMap = new HashMap<String, ZNode>();
    Map<String, ZNode> cache = new HashMap<String, ZNode>();
    for (String path : paths) {
      readZkRecursive(path, zkMap, zkclient);
      readZkRecursive(path, cache, zkAccessor);
    }
    // printCache(map);

    return verifyZkCache(paths, null, cache, zkMap, needVerifyStat);
  }

  public static boolean verifyZkCache(List<String> paths, Map<String, ZNode> cache,
      ZkClient zkclient, boolean needVerifyStat) {
    return verifyZkCache(paths, null, cache, zkclient, needVerifyStat);
  }

  public static boolean verifyZkCache(List<String> paths, List<String> pathsExcludeForStat,
      Map<String, ZNode> cache, ZkClient zkclient, boolean needVerifyStat) {
    // read everything on zk under paths
    Map<String, ZNode> zkMap = new HashMap<String, ZNode>();
    for (String path : paths) {
      readZkRecursive(path, zkMap, zkclient);
    }
    // printCache(map);

    return verifyZkCache(paths, pathsExcludeForStat, cache, zkMap, needVerifyStat);
  }

  public static boolean verifyZkCache(List<String> paths, List<String> pathsExcludeForStat,
      Map<String, ZNode> cache, Map<String, ZNode> zkMap, boolean needVerifyStat) {
    // equal size
    if (zkMap.size() != cache.size()) {
      System.err.println("size mismatch: cacheSize: " + cache.size() + ", zkMapSize: "
          + zkMap.size());
      System.out.println("cache: (" + cache.size() + ")");
      TestHelper.printCache(cache);

      System.out.println("zkMap: (" + zkMap.size() + ")");
      TestHelper.printCache(zkMap);

      return false;
    }

    // everything in cache is also in map
    for (String path : cache.keySet()) {
      ZNode cacheNode = cache.get(path);
      ZNode zkNode = zkMap.get(path);

      if (zkNode == null) {
        // in cache but not on zk
        System.err.println("path: " + path + " in cache but not on zk: inCacheNode: " + cacheNode);
        return false;
      }

      if ((zkNode.getData() == null && cacheNode.getData() != null)
          || (zkNode.getData() != null && cacheNode.getData() == null)
          || (zkNode.getData() != null && cacheNode.getData() != null && !zkNode.getData().equals(
              cacheNode.getData()))) {
        // data not equal
        System.err.println("data mismatch on path: " + path + ", inCache: " + cacheNode.getData()
            + ", onZk: " + zkNode.getData());
        return false;
      }

      if ((zkNode.getChildSet() == null && cacheNode.getChildSet() != null)
          || (zkNode.getChildSet() != null && cacheNode.getChildSet() == null)
          || (zkNode.getChildSet() != null && cacheNode.getChildSet() != null && !zkNode
              .getChildSet().equals(cacheNode.getChildSet()))) {
        // childSet not equal
        System.err.println("childSet mismatch on path: " + path + ", inCache: "
            + cacheNode.getChildSet() + ", onZk: " + zkNode.getChildSet());
        return false;
      }

      if (needVerifyStat && pathsExcludeForStat != null && !pathsExcludeForStat.contains(path)) {
        if (cacheNode.getStat() == null || !zkNode.getStat().equals(cacheNode.getStat())) {
          // stat not equal
          System.err.println("Stat mismatch on path: " + path + ", inCache: " + cacheNode.getStat()
              + ", onZk: " + zkNode.getStat());
          return false;
        }
      }
    }

    return true;
  }

  public static StateModelDefinition generateStateModelDefForBootstrap() {
    ZNRecord record = new ZNRecord("Bootstrap");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "IDLE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("ONLINE");
    statePriorityList.add("BOOTSTRAP");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("IDLE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("ONLINE")) {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      } else if (state.equals("BOOTSTRAP")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("IDLE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("DROPPED")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }

    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("ONLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("BOOTSTRAP", "OFFLINE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        metadata.put("IDLE", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("BOOTSTRAP")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "ONLINE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        metadata.put("IDLE", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "BOOTSTRAP");
        metadata.put("BOOTSTRAP", "BOOTSTRAP");
        metadata.put("DROPPED", "IDLE");
        metadata.put("IDLE", "IDLE");
        record.setMapField(key, metadata);
      } else if (state.equals("IDLE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "OFFLINE");
        metadata.put("BOOTSTRAP", "OFFLINE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("IDLE", "IDLE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("ONLINE-OFFLINE");
    stateTransitionPriorityList.add("BOOTSTRAP-ONLINE");
    stateTransitionPriorityList.add("OFFLINE-BOOTSTRAP");
    stateTransitionPriorityList.add("BOOTSTRAP-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-IDLE");
    stateTransitionPriorityList.add("IDLE-OFFLINE");
    stateTransitionPriorityList.add("IDLE-DROPPED");
    stateTransitionPriorityList.add("ERROR-IDLE");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return new StateModelDefinition(record);
  }

  public static String znrecordToString(ZNRecord record) {
    StringBuffer sb = new StringBuffer();
    sb.append(record.getId() + "\n");
    Map<String, String> simpleFields = record.getSimpleFields();
    if (simpleFields != null) {
      sb.append("simpleFields\n");
      for (String key : simpleFields.keySet()) {
        sb.append("  " + key + "\t: " + simpleFields.get(key) + "\n");
      }
    }

    Map<String, List<String>> listFields = record.getListFields();
    sb.append("listFields\n");
    for (String key : listFields.keySet()) {
      List<String> list = listFields.get(key);
      sb.append("  " + key + "\t: ");
      for (String listValue : list) {
        sb.append(listValue + ", ");
      }
      sb.append("\n");
    }

    Map<String, Map<String, String>> mapFields = record.getMapFields();
    sb.append("mapFields\n");
    for (String key : mapFields.keySet()) {
      Map<String, String> map = mapFields.get(key);
      sb.append("  " + key + "\t: \n");
      for (String mapKey : map.keySet()) {
        sb.append("    " + mapKey + "\t: " + map.get(mapKey) + "\n");
      }
    }

    return sb.toString();
  }

  public static interface Verifier {
    boolean verify() throws Exception;
  }

  public static boolean verify(Verifier verifier, long timeout) throws Exception {
    long start = System.currentTimeMillis();
    do {
      boolean result = verifier.verify();
      if (result || (System.currentTimeMillis() - start) > timeout) {
        return result;
      }
      Thread.sleep(100);
    } while (true);
  }

  public static void printHandlers(HelixManager manager, List<ZkCallbackHandler> handlers) {
    StringBuilder sb = new StringBuilder();
    sb.append(manager.getInstanceName() + " has " + handlers.size() + " cb-handlers. [\n");

    for (int i = 0; i < handlers.size(); i++) {
      ZkCallbackHandler handler = handlers.get(i);
      String path = handler.getPath();
      sb.append(path.substring(manager.getClusterName().length() + 1) + ": "
          + handler.getListener());
      if (i < (handlers.size() - 1)) {
        sb.append("\n");
      }
    }
    sb.append("]");

    System.out.println(sb.toString());
  }

  public static int getRandomPort() throws IOException {
    ServerSocket sock = new ServerSocket();
    sock.bind(null);
    int port = sock.getLocalPort();
    sock.close();
    return port;
  }

}
