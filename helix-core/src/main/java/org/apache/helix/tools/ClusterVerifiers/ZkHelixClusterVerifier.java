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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.manager.zk.GenericZkHelixApiBuilder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkHelixClusterVerifier
    implements IZkChildListener, IZkDataListener, HelixClusterVerifier, AutoCloseable {
  private static Logger LOG = LoggerFactory.getLogger(ZkHelixClusterVerifier.class);
  protected static int DEFAULT_TIMEOUT = 300 * 1000;
  protected static int DEFAULT_PERIOD = 500;

  protected final RealmAwareZkClient _zkClient;
  // true if ZkHelixClusterVerifier was instantiated with a RealmAwareZkClient, false otherwise
  // This is used for close() to determine how ZkHelixClusterVerifier should close the underlying
  // ZkClient
  private final boolean _usesExternalZkClient;
  protected final String _clusterName;
  protected final HelixDataAccessor _accessor;
  protected final PropertyKey.Builder _keyBuilder;
  private CountDownLatch _countdown;
  protected final int _waitPeriodTillVerify;

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

  protected ZkHelixClusterVerifier(RealmAwareZkClient zkClient, String clusterName,
      boolean usesExternalZkClient, int waitPeriodTillVerify) {
    if (zkClient == null || clusterName == null) {
      throw new IllegalArgumentException("requires zkClient|clusterName");
    }
    _zkClient = zkClient;
    _usesExternalZkClient = usesExternalZkClient;
    _clusterName = clusterName;
    _accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    _keyBuilder = _accessor.keyBuilder();
    _waitPeriodTillVerify = waitPeriodTillVerify;
  }

  @Deprecated
  public ZkHelixClusterVerifier(String zkAddr, String clusterName, int waitPeriodTillVerify) {
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("ZkHelixClusterVerifier: clusterName is null or empty!");
    }
    // If the multi ZK config is enabled, use DedicatedZkClient on multi-realm mode
    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddr == null) {
      LOG.info(
          "ZkHelixClusterVerifier: zkAddr is null or multi-zk mode is enabled in System Properties."
              + " Instantiating in multi-zk mode!");
      try {
        RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
        connectionConfigBuilder.setZkRealmShardingKey("/" + clusterName);
        RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
            new RealmAwareZkClient.RealmAwareZkClientConfig();
        _zkClient = DedicatedZkClientFactory.getInstance()
            .buildZkClient(connectionConfigBuilder.build(), clientConfig);
      } catch (InvalidRoutingDataException | IllegalStateException e) {
        // Note: IllegalStateException is for HttpRoutingDataReader if MSDS endpoint cannot be
        // found
        throw new HelixException("ZkHelixClusterVerifier: failed to create ZkClient!", e);
      }
    } else {
      _zkClient = DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    }
    _usesExternalZkClient = false;
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _clusterName = clusterName;
    _accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_zkClient));
    _keyBuilder = _accessor.keyBuilder();
    _waitPeriodTillVerify = waitPeriodTillVerify;
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

  protected void waitTillVerify() {
    try {
      if (_waitPeriodTillVerify != 0) {
        Thread.sleep(_waitPeriodTillVerify);
      }
    } catch (InterruptedException e) {
      LOG.error("cooldown in verifyByPolling interrupted");
    }
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
    waitTillVerify();

    try {
      long start = System.currentTimeMillis();
      boolean success;
      do {
        success = verifyState();
        if (success) {
          return true;
        }
        TimeUnit.MILLISECONDS.sleep(period);
      } while ((System.currentTimeMillis() - start) <= timeout);
      LOG.error("verifier timeout out with timeout {}", timeout);
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

  /**
   * Implement close() for {@link AutoCloseable} and {@link HelixClusterVerifier}.
   * Non-external resources should be closed in this method to prevent resource leak.
   */
  @Override
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
          if (!success) {
            LOG.error("verifyByCallback failed due to timeout {}", timeout);
          }
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

  public String getClusterName() {
    return _clusterName;
  }

  protected abstract static class Builder<B extends Builder<B>> extends GenericZkHelixApiBuilder<B> {
    protected int _waitPeriodTillVerify;

    public Builder() {
      // Note: ZkHelixClusterVerifier is a single-realm API, so RealmMode is assumed to be
      // SINGLE-REALM
      setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM);
    }

    /**
     * Use setZkAddress() instead. Deprecated but left here for backward-compatibility.
     * @param zkAddress
     * @return
     */
    @Deprecated
    public B setZkAddr(String zkAddress) {
      return setZkAddress(zkAddress);
    }

    /**
     * The class of verify() methods in this class and its subclass such as
     * BestPossibleExternalViewVerifier is intend to wait for the cluster converging to a stable
     * state after changes in the cluster. However, after making changes, it would take some time
     * till controller taking the changes in. Thus, if we verify() too early, before controller
     * taking the changes, the class may mistake the previous stable cluster state as new (expected)
     * stable state. This would cause various issues. Thus, we supply a waitPeriod before starting
     * to validate next expected state to avoid this pre-mature stable state validation.
     */
    public B setWaitTillVerify(int waitPeriod) {
      _waitPeriodTillVerify = waitPeriod;
      return (B) this;
    }

    public String getClusterName() {
      if (_realmAwareZkConnectionConfig != null && (
          _realmAwareZkConnectionConfig.getZkRealmShardingKey() != null
              && !_realmAwareZkConnectionConfig.getZkRealmShardingKey().isEmpty())) {
        // Need to remove the first "/" from sharding key given
        return _realmAwareZkConnectionConfig.getZkRealmShardingKey().substring(1);
      }
      throw new HelixException(
          "Failed to get the cluster name! Either RealmAwareZkConnectionConfig is null or its sharding key is null or empty!");
    }

    protected void validate() {
      // Validate that either ZkAddress or ZkRealmShardingKey is set
      if (_zkAddress == null || _zkAddress.isEmpty()) {
        if (_realmAwareZkConnectionConfig == null
            || _realmAwareZkConnectionConfig.getZkRealmShardingKey() == null
            || _realmAwareZkConnectionConfig.getZkRealmShardingKey().isEmpty()) {
          throw new IllegalArgumentException(
              "ZkHelixClusterVerifier: one of either ZkAddress or ZkRealmShardingKey must be set! ZkAddress: "
                  + _zkAddress + " RealmAwareZkConnectionConfig: " + _realmAwareZkConnectionConfig);
        }
      }
      initializeConfigsIfNull();
    }

    /**
     * Creates a RealmAwareZkClient for ZkHelixClusterVerifiers.
     * Note that DedicatedZkClient is used whether it's multi-realm or single-realm.
     * @return
     */
    @Override
    protected RealmAwareZkClient createZkClient(RealmAwareZkClient.RealmMode realmMode,
        RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
        RealmAwareZkClient.RealmAwareZkClientConfig clientConfig, String zkAddress) {
      if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddress == null) {
        try {
          // First, try to create a RealmAwareZkClient that's a DedicatedZkClient
          return DedicatedZkClientFactory.getInstance()
              .buildZkClient(connectionConfig, clientConfig);
        } catch (InvalidRoutingDataException | IllegalStateException e) {
          throw new HelixException("ZkHelixClusterVerifier: failed to create ZkClient!", e);
        }
      } else {
        return DedicatedZkClientFactory.getInstance()
            .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress));
      }
    }
  }

  @Override
  public void finalize() {
    close();
  }
}
