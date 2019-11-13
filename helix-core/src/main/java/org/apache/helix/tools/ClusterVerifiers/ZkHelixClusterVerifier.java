package org.apache.helix.tools.ClusterVerifiers;

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

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class ZkHelixClusterVerifier
    implements IZkChildListener, IZkDataListener, HelixClusterVerifier {
  private static Logger LOG = LoggerFactory.getLogger(ZkHelixClusterVerifier.class);
  protected static int DEFAULT_TIMEOUT = 300 * 1000;
  protected static int DEFAULT_PERIOD = 500;

  protected final HelixZkClient _zkClient;
  // true if ZkHelixClusterVerifier was instantiated with a HelixZkClient, false otherwise
  // This is used for close() to determine how ZkHelixClusterVerifier should close the underlying
  // ZkClient
  private boolean _usesExternalZkClient;
  protected final String _clusterName;
  protected final HelixDataAccessor _accessor;
  protected final PropertyKey.Builder _keyBuilder;
  private CountDownLatch _countdown;

  private ExecutorService _verifyTaskThreadPool =
      Executors.newSingleThreadExecutor(r -> new Thread(r, "ZkHelixClusterVerifier-verify_thread"));

  protected static class ClusterVerifyTrigger {
    final PropertyKey _triggerKey;
    final boolean _triggerOnDataChange;
    final boolean _triggerOnChildChange;
    final boolean _triggerOnChildDataChange;

    public ClusterVerifyTrigger(PropertyKey triggerKey, boolean triggerOnDataChange,
        boolean triggerOnChildChange, boolean triggerOnChildDataChange) {
      _triggerKey = triggerKey;
      _triggerOnDataChange = triggerOnDataChange;
      _triggerOnChildChange = triggerOnChildChange;
      _triggerOnChildDataChange = triggerOnChildDataChange;
    }

    public boolean isTriggerOnDataChange() {
      return _triggerOnDataChange;
    }

    public PropertyKey getTriggerKey() {
      return _triggerKey;
    }

    public boolean isTriggerOnChildChange() {
      return _triggerOnChildChange;
    }

    public boolean isTriggerOnChildDataChange() {
      return _triggerOnChildDataChange;
    }
  }

  public ZkHelixClusterVerifier(HelixZkClient zkClient, String clusterName) {
    if (zkClient == null || clusterName == null) {
      throw new IllegalArgumentException("requires zkClient|clusterName");
    }
    _zkClient = zkClient;
    _usesExternalZkClient = true;
    _clusterName = clusterName;
    _accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    _keyBuilder = _accessor.keyBuilder();
  }

  public ZkHelixClusterVerifier(String zkAddr, String clusterName) {
    if (zkAddr == null || clusterName == null) {
      throw new IllegalArgumentException("requires zkAddr|clusterName");
    }
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    _usesExternalZkClient = false;
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _clusterName = clusterName;
    _accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    _keyBuilder = _accessor.keyBuilder();
  }

  /**
   * Verify the cluster.
   * The method will be blocked at most {@code timeout}.
   * Return true if the verify succeed, otherwise return false.
   * @param timeout in milliseconds
   * @return true if succeed, false if not.
   */
  public boolean verify(long timeout) {
    return verifyByZkCallback(timeout);
  }

  /**
   * Verify the cluster.
   * The method will be blocked at most 30 seconds.
   * Return true if the verify succeed, otherwise return false.
   * @return true if succeed, false if not.
   */
  public boolean verify() {
    return verify(DEFAULT_TIMEOUT);
  }

  /**
   * Verify the cluster by relying on zookeeper callback and verify.
   * The method will be blocked at most {@code timeout}.
   * Return true if the verify succeed, otherwise return false.
   * @param timeout in milliseconds
   * @return true if succeed, false if not.
   */
  public abstract boolean verifyByZkCallback(long timeout);

  /**
   * Verify the cluster by relying on zookeeper callback and verify.
   * The method will be blocked at most 30 seconds.
   * Return true if the verify succeed, otherwise return false.
   * @return true if succeed, false if not.
   */
  public boolean verifyByZkCallback() {
    return verifyByZkCallback(DEFAULT_TIMEOUT);
  }

  /**
   * Verify the cluster by periodically polling the cluster status and verify.
   * The method will be blocked at most {@code timeout}.
   * Return true if the verify succeed, otherwise return false.
   * @param timeout
   * @param period polling interval
   * @return
   */
  public boolean verifyByPolling(long timeout, long period) {
    try {
      long start = System.currentTimeMillis();
      boolean success;
      do {
        // Add a rebalance invoker in case some callbacks got buried - sometimes callbacks get
        // processed even before changes get fully written to ZK.
        invokeRebalance(_accessor);

        success = verifyState();
        if (success) {
          return true;
        }
        TimeUnit.MILLISECONDS.sleep(period);
      } while ((System.currentTimeMillis() - start) <= timeout);
    } catch (Exception e) {
      LOG.error("Exception in verifier", e);
    }
    return false;
  }

  /**
   * Verify the cluster by periodically polling the cluster status and verify.
   * The method will be blocked at most 30 seconds.
   * Return true if the verify succeed, otherwise return false.
   * @return true if succeed, false if not.
   */
  public boolean verifyByPolling() {
    return verifyByPolling(DEFAULT_TIMEOUT, DEFAULT_PERIOD);
  }

  public void close() {
    if (_zkClient != null && !_usesExternalZkClient) {
      _zkClient.close();
    }
  }

  protected boolean verifyByCallback(long timeout, List<ClusterVerifyTrigger> triggers) {
    _countdown = new CountDownLatch(1);

    for (ClusterVerifyTrigger trigger : triggers) {
      subscribeTrigger(trigger);
    }

    boolean success = false;
    try {
      success = verifyState();
      if (!success) {
        success = _countdown.await(timeout, TimeUnit.MILLISECONDS);
        if (!success) {
          // make a final try if timeout
          success = verifyState();
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in verifier", e);
    }

    // clean up
    _zkClient.unsubscribeAll();
    _verifyTaskThreadPool.shutdownNow();

    return success;
  }

  private void subscribeTrigger(ClusterVerifyTrigger trigger) {
    String path = trigger.getTriggerKey().getPath();
    if (trigger.isTriggerOnDataChange()) {
      _zkClient.subscribeDataChanges(path, this);
    }

    if (trigger.isTriggerOnChildChange()) {
      _zkClient.subscribeChildChanges(path, this);
    }

    if (trigger.isTriggerOnChildDataChange()) {
      List<String> childs = _zkClient.getChildren(path);
      for (String child : childs) {
        String childPath = String.format("%s/%s", path, child);
        _zkClient.subscribeDataChanges(childPath, this);
      }
    }
  }

  /**
   * The method actually performs the required verifications.
   * @return
   * @throws Exception
   */
  protected abstract boolean verifyState() throws Exception;

  class VerifyStateCallbackTask implements Runnable {
    @Override
    public void run() {
      try {
        boolean success = verifyState();
        if (success) {
          _countdown.countDown();
        }
      } catch (Exception ex) {
        LOG.info("verifyState() throws exception: " + ex);
      }
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void handleDataChange(String dataPath, Object data) throws Exception {
    if (!_verifyTaskThreadPool.isShutdown()) {
      _verifyTaskThreadPool.submit(new VerifyStateCallbackTask());
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    _zkClient.unsubscribeDataChanges(dataPath, this);
    if (!_verifyTaskThreadPool.isShutdown()) {
      _verifyTaskThreadPool.submit(new VerifyStateCallbackTask());
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    for (String child : currentChilds) {
      String childPath = String.format("%s/%s", parentPath, child);
      _zkClient.subscribeDataChanges(childPath, this);
    }
    if (!_verifyTaskThreadPool.isShutdown()) {
      _verifyTaskThreadPool.submit(new VerifyStateCallbackTask());
    }
  }

  public HelixZkClient getHelixZkClient() {
    return _zkClient;
  }

  @Deprecated
  public ZkClient getZkClient() {
    return (ZkClient) getHelixZkClient();
  }

  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Invoke a cluster rebalance in case some callbacks get ignored. This is for Helix integration
   * testing purposes only.
   */
  public static synchronized void invokeRebalance(HelixDataAccessor accessor) {
    String dummyName = UUID.randomUUID().toString();
    ResourceConfig dummyConfig = new ResourceConfig(dummyName);
    accessor.updateProperty(accessor.keyBuilder().resourceConfig(dummyName), dummyConfig);
    accessor.removeProperty(accessor.keyBuilder().resourceConfig(dummyName));
  }
}
