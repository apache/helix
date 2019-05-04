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

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * test zookeeper basis
 */
public class TestZkBasis extends ZkUnitTestBase {
  class ZkListener implements IZkDataListener, IZkChildListener {
    String _parentPath = null;
    String _dataDeletePath = null;
    List<String> _currentChilds = Collections.emptyList(); // make sure it's set to null in
                                                           // #handleChildChange()

    CountDownLatch _childChangeCountDown = new CountDownLatch(1);
    CountDownLatch _dataDeleteCountDown = new CountDownLatch(1);

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) {
      _parentPath = parentPath;
      _currentChilds = currentChilds;
      _childChangeCountDown.countDown();
    }

    @Override
    public void handleDataChange(String dataPath, Object data) {
      // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      _dataDeletePath = dataPath;
      _dataDeleteCountDown.countDown();
    }
  }

  @Test
  public void testZkSessionExpiry() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZkClient client = new ZkClient(ZK_ADDR, HelixZkClient.DEFAULT_SESSION_TIMEOUT,
        HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

    String path = String.format("/%s", clusterName);
    client.createEphemeral(path);
    String oldSessionId = ZkTestHelper.getSessionId(client);
    ZkTestHelper.expireSession(client);
    String newSessionId = ZkTestHelper.getSessionId(client);
    Assert.assertNotSame(newSessionId, oldSessionId);
    Assert.assertFalse(client.exists(path), "Ephemeral znode should be gone after session expiry");
    client.close();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCloseZkClient() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZkClient client = new ZkClient(ZK_ADDR, HelixZkClient.DEFAULT_SESSION_TIMEOUT,
        HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String path = String.format("/%s", clusterName);
    client.createEphemeral(path);

    client.close();
    Assert.assertFalse(_gZkClient.exists(path),
        "Ephemeral node: " + path + " should be removed after ZkClient#close()");
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCloseZkClientInZkClientEventThread() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    final CountDownLatch waitCallback = new CountDownLatch(1);
    final ZkClient client = new ZkClient(ZK_ADDR, HelixZkClient.DEFAULT_SESSION_TIMEOUT,
        HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String path = String.format("/%s", clusterName);
    client.createEphemeral(path);
    client.subscribeDataChanges(path, new IZkDataListener() {

      @Override
      public void handleDataDeleted(String dataPath) {
      }

      @Override
      public void handleDataChange(String dataPath, Object data) {
        client.close();
        waitCallback.countDown();
      }
    });

    client.writeData(path, new ZNRecord("test"));
    waitCallback.await();
    Assert.assertFalse(_gZkClient.exists(path), "Ephemeral node: " + path
        + " should be removed after ZkClient#close() in its own event-thread");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  /**
   * test zk watchers are renewed automatically after session expiry
   * zookeeper-client side keeps all registered watchers see ZooKeeper.WatchRegistration.register()
   * after session expiry, all watchers are renewed
   * if a path that has watches on it has been removed during session expiry,
   * the watchers on that path will still get callbacks after session renewal, especially:
   * a data-watch will get data-deleted callback
   * a child-watch will get a child-change callback with current-child-list = null
   * this can be used for cleanup watchers on the zookeeper-client side
   */
  @Test
  public void testWatchRenew() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    final ZkClient client = new ZkClient(ZK_ADDR, HelixZkClient.DEFAULT_SESSION_TIMEOUT,
        HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    // make sure "/testName/test" doesn't exist
    final String path = "/" + testName + "/test";
    client.delete(path);

    ZkListener listener = new ZkListener();
    client.subscribeDataChanges(path, listener);
    client.subscribeChildChanges(path, listener);

    ZkTestHelper.expireSession(client);

    boolean succeed = listener._childChangeCountDown.await(10, TimeUnit.SECONDS);
    Assert.assertTrue(succeed,
        "fail to wait on child-change count-down in 10 seconds after session-expiry");
    Assert.assertEquals(listener._parentPath, path,
        "fail to get child-change callback after session-expiry");
    Assert.assertNull(listener._currentChilds,
        "fail to get child-change callback with currentChilds=null after session expiry");

    succeed = listener._dataDeleteCountDown.await(10, TimeUnit.SECONDS);
    Assert.assertTrue(succeed,
        "fail to wait on data-delete count-down in 10 seconds after session-expiry");
    Assert.assertEquals(listener._dataDeletePath, path,
        "fail to get data-delete callback after session-expiry");

    client.close();
  }

  /**
   * after calling zkclient#unsubscribeXXXListener()
   * an already registered watch will not be removed from ZooKeeper#watchManager#XXXWatches
   * immediately.
   * the watch will get removed on the following conditions:
   * 1) there is a set/delete on the listening path via the zkclient
   * 2) session expiry on the zkclient (i.e. the watch will not be renewed after session expiry)
   * @throws Exception
   */
  @Test
  public void testWatchRemove() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    final ZkClient client = new ZkClient(ZK_ADDR, HelixZkClient.DEFAULT_SESSION_TIMEOUT,
        HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    // make sure "/testName/test" doesn't exist
    final String path = "/" + testName + "/test";
    client.createPersistent(path, true);

    ZkListener listener = new ZkListener();
    client.subscribeDataChanges(path, listener);
    client.subscribeChildChanges(path, listener);

    // listener should be in both ZkClient#_dataListener and ZkClient#_childListener set
    Map<String, Set<IZkDataListener>> dataListenerMap = ZkTestHelper.getZkDataListener(client);
    Assert.assertEquals(dataListenerMap.size(), 1, "ZkClient#_dataListener should have 1 listener");
    Set<IZkDataListener> dataListenerSet = dataListenerMap.get(path);
    Assert.assertNotNull(dataListenerSet,
        "ZkClient#_dataListener should have 1 listener on path: " + path);
    Assert.assertEquals(dataListenerSet.size(), 1,
        "ZkClient#_dataListener should have 1 listener on path: " + path);

    Map<String, Set<IZkChildListener>> childListenerMap = ZkTestHelper.getZkChildListener(client);
    Assert.assertEquals(childListenerMap.size(), 1,
        "ZkClient#_childListener should have 1 listener");
    Set<IZkChildListener> childListenerSet = childListenerMap.get(path);
    Assert.assertNotNull(childListenerSet,
        "ZkClient#_childListener should have 1 listener on path: " + path);
    Assert.assertEquals(childListenerSet.size(), 1,
        "ZkClient#_childListener should have 1 listener on path: " + path);

    // watch should be in ZooKeeper#watchManager#XXXWatches
    Map<String, List<String>> watchMap = ZkTestHelper.getZkWatch(client);
    // System.out.println("watchMap1: " + watchMap);
    List<String> dataWatch = watchMap.get("dataWatches");
    Assert.assertNotNull(dataWatch,
        "ZooKeeper#watchManager#dataWatches should have 1 data watch on path: " + path);
    Assert.assertEquals(dataWatch.size(), 1,
        "ZooKeeper#watchManager#dataWatches should have 1 data watch on path: " + path);
    Assert.assertEquals(dataWatch.get(0), path,
        "ZooKeeper#watchManager#dataWatches should have 1 data watch on path: " + path);

    List<String> childWatch = watchMap.get("childWatches");
    Assert.assertNotNull(childWatch,
        "ZooKeeper#watchManager#childWatches should have 1 child watch on path: " + path);
    Assert.assertEquals(childWatch.size(), 1,
        "ZooKeeper#watchManager#childWatches should have 1 child watch on path: " + path);
    Assert.assertEquals(childWatch.get(0), path,
        "ZooKeeper#watchManager#childWatches should have 1 child watch on path: " + path);

    client.unsubscribeDataChanges(path, listener);
    client.unsubscribeChildChanges(path, listener);
    // System.out.println("watchMap2: " + watchMap);
    ZkTestHelper.expireSession(client);

    // after session expiry, those watches should be removed
    watchMap = ZkTestHelper.getZkWatch(client);
    // System.out.println("watchMap3: " + watchMap);
    dataWatch = watchMap.get("dataWatches");
    Assert.assertTrue(dataWatch.isEmpty(), "ZooKeeper#watchManager#dataWatches should be empty");
    childWatch = watchMap.get("childWatches");
    Assert.assertTrue(childWatch.isEmpty(), "ZooKeeper#watchManager#childWatches should be empty");

    client.close();
    deleteCluster(testName);
  }
}
