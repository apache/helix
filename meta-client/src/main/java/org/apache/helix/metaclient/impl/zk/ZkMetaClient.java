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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.helix.metaclient.api.AsyncCallback;
import org.apache.helix.metaclient.api.ChildChangeListener;
import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.DataUpdater;
import org.apache.helix.metaclient.api.DirectChildChangeListener;
import org.apache.helix.metaclient.api.DirectChildSubscribeResult;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.api.Op;
import org.apache.helix.metaclient.api.OpResult;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.helix.metaclient.impl.zk.adapter.ChildListenerAdapter;
import org.apache.helix.metaclient.impl.zk.adapter.DataListenerAdapter;
import org.apache.helix.metaclient.impl.zk.adapter.DirectChildListenerAdapter;
import org.apache.helix.metaclient.impl.zk.adapter.StateChangeListenerAdapter;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientCreateCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientDeleteCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientExistCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientGetCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientSetCallbackHandler;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil;
import org.apache.helix.zookeeper.api.client.ChildrenSubscribeResult;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil.convertZkEntryModeToMetaClientEntryMode;
import static org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil.translateZkExceptionToMetaclientException;


public class ZkMetaClient<T> implements MetaClientInterface<T>, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClient.class);
  private final ZkClient _zkClient;
  private final long _initConnectionTimeout;
  private final long _reconnectTimeout;

  // After ZkClient gets disconnected from ZK server, it keeps retrying connection until connection
  // is re-established or ZkClient is closed. We need a separate thread to monitor ZkClient
  // reconnect and close ZkClient if it not able to reconnect within user specified timeout.
  private final ScheduledExecutorService _zkClientReconnectMonitor;
  private ScheduledFuture<?> _reconnectMonitorFuture;
  private ReconnectStateChangeListener _reconnectStateChangeListener;
  // Lock all activities related to ZkClient connection
  private ReentrantLock _zkClientConnectionMutex = new ReentrantLock();


  public ZkMetaClient(ZkMetaClientConfig config) {
    _initConnectionTimeout = config.getConnectionInitTimeoutInMillis();
    _reconnectTimeout = config.getMetaClientReconnectPolicy().getAutoReconnectTimeout();
    // TODO: Right new ZkClient reconnect using exp backoff with fixed max backoff interval. We should
    // Allow user to config reconnect policy
    _zkClient = new ZkClient(
        new ZkConnection(config.getConnectionAddress(), (int) config.getSessionTimeoutInMillis()),
        (int) _initConnectionTimeout, _reconnectTimeout /*use reconnect timeout for retry timeout*/,
        config.getZkSerializer(), config.getMonitorType(), config.getMonitorKey(),
        config.getMonitorInstanceName(), config.getMonitorRootPathOnly(), false, true);
    _zkClientReconnectMonitor = Executors.newSingleThreadScheduledExecutor();
    _reconnectStateChangeListener = new ReconnectStateChangeListener();
  }

  @Override
  public void create(String key, Object data) {
    try {
      create(key, data, EntryMode.PERSISTENT);
    } catch (Exception e) {
      throw new MetaClientException(e);
    }
  }

  @Override
  public void create(String key, Object data, MetaClientInterface.EntryMode mode) {

    try{
      _zkClient.create(key, data, ZkMetaClientUtil.convertMetaClientMode(mode));
    } catch (ZkException | KeeperException e) {
      throw new MetaClientException(e);
    }
  }

  @Override
  public void createWithTTL(String key, T data, long ttl) {
    try{
      _zkClient.createPersistentWithTTL(key, data, ttl);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public void renewTTLNode(String key) {
    T oldData = get(key);
    if (oldData == null) {
      throw new MetaClientNoNodeException("Node at " + key + " does not exist.");
    }
    set(key, oldData, _zkClient.getStat(key).getVersion());
  }

  @Override
  public void set(String key, T data, int version) {
    try {
      _zkClient.writeData(key, data, version);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public T update(String key, DataUpdater<T> updater) {
    org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
    // TODO: add retry logic for ZkBadVersionException.
    try {
      T oldData = _zkClient.readData(key, stat);
      T newData = updater.update(oldData);
      set(key, newData, stat.getVersion());
      return newData;
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  //TODO: Get Expiry Time in Stat
  @Override
  public Stat exists(String key) {
    org.apache.zookeeper.data.Stat zkStats;
    try {
      zkStats = _zkClient.getStat(key);
      if (zkStats == null) {
        return null;
      }
      return ZkMetaClientUtil.convertZkStatToStat(zkStats);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public T get(String key) {
    return _zkClient.readData(key, true);
  }


  @Override
  public ImmutablePair<T, Stat> getDataAndStat(final String key) {
    try {
      org.apache.zookeeper.data.Stat zkStat = new org.apache.zookeeper.data.Stat();
      T data = _zkClient.readData(key, zkStat);
      return ImmutablePair.of(data, ZkMetaClientUtil.convertZkStatToStat(zkStat));
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public List<String> getDirectChildrenKeys(String key) {
    try {
      return _zkClient.getChildren(key);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public int countDirectChildren(String key) {
    return _zkClient.countChildren(key);
  }

  @Override
  public boolean delete(String key) {
    try {
      return _zkClient.delete(key);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public boolean recursiveDelete(String key) {
    _zkClient.deleteRecursively(key);
    return true;
  }

  // In Current ZkClient, Async CRUD do auto retry when connection lost or session mismatch using
  // existing retry handling logic in zkClient. (defined in ZkAsyncCallbacks)
  // ZkClient execute async callbacks at zkClient main thead, retry is handles in a separate retry
  // thread. In our first version of implementation, we will keep similar behavior and have
  // callbacks executed in ZkClient event thread, and reuse zkClient retry logic.

  // It is highly recommended *NOT* to perform any blocking operation inside the callbacks.
  // If you block the thread the meta client won't process other events.

  // corresponding callbacks for each operation are invoked in order.
  @Override
  public void setAsyncExecPoolSize(int poolSize) {
    throw new UnsupportedOperationException(
        "All async calls are executed in a single thread to maintain sequence.");
  }

  @Override
  public void asyncCreate(String key, Object data, EntryMode mode, AsyncCallback.VoidCallback cb) {
    CreateMode entryMode;
    try {
      entryMode = ZkMetaClientUtil.convertMetaClientMode(mode);
    } catch (ZkException | KeeperException e) {
      throw new MetaClientException(e);
    }
    _zkClient.asyncCreate(key, data, entryMode,
          new ZkMetaClientCreateCallbackHandler(cb));
  }

  @Override
  public void asyncUpdate(String key, DataUpdater<T> updater, AsyncCallback.DataCallback cb) {
    throw new NotImplementedException("Currently asyncUpdate is not supported in ZkMetaClient.");
    /*
     * TODO:  Only Helix has potential using this API as of now.  (ZkBaseDataAccessor.update())
     *  Will move impl from ZkBaseDataAccessor to here when retiring ZkBaseDataAccessor.
     */
  }

  @Override
  public void asyncGet(String key, AsyncCallback.DataCallback cb) {
    _zkClient.asyncGetData(key,
        new ZkMetaClientGetCallbackHandler(cb));
  }

  @Override
  public void asyncCountChildren(String key, AsyncCallback.DataCallback cb) {
    throw new NotImplementedException(
        "Currently asyncCountChildren is not supported in ZkMetaClient.");
    /*
     * TODO:  Only Helix has potential using this API as of now. (ZkBaseDataAccessor.getChildren())
     *  Will move impl from ZkBaseDataAccessor to here when retiring ZkBaseDataAccessor.
     */

  }

  @Override
  public void asyncExist(String key, AsyncCallback.StatCallback cb) {
    _zkClient.asyncExists(key,
        new ZkMetaClientExistCallbackHandler(cb));
  }

  public void asyncDelete(String key, AsyncCallback.VoidCallback cb) {
    _zkClient.asyncDelete(key, new ZkMetaClientDeleteCallbackHandler(cb));
  }

  @Override
  public void asyncTransaction(Iterable<Op> ops, AsyncCallback.TransactionCallback cb) {
    throw new NotImplementedException(
        "Currently asyncTransaction is not supported in ZkMetaClient.");

     //TODO: There is no active use case for Async transaction.
  }

  @Override
  public void asyncSet(String key, T data, int version, AsyncCallback.StatCallback cb) {
    _zkClient.asyncSetData(key, data, version,
        new ZkMetaClientSetCallbackHandler(cb));
  }

  @Override
  public void connect() {
    try {
      _zkClientConnectionMutex.lock();
      _zkClient.connect(_initConnectionTimeout, _zkClient);
      // register _reconnectStateChangeListener as state change listener to react to ZkClient connect
      // state change event. When ZkClient disconnected from ZK, it still auto reconnect until
      // ZkClient is closed or connection re-established.
      // We will need to close ZkClient when user set retry connection timeout.
      _zkClient.subscribeStateChanges(_reconnectStateChangeListener);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    } finally {
      _zkClientConnectionMutex.unlock();
    }
  }

  @Override
  public void disconnect() {
    cleanUpAndClose(true, true);
    _zkClientReconnectMonitor.shutdownNow();
  }

  @Override
  public ConnectState getClientConnectionState() {
    return null;
  }

  @Override
  public boolean subscribeDataChange(String key, DataChangeListener listener, boolean skipWatchingNonExistNode) {
    _zkClient.subscribeDataChanges(key, new DataListenerAdapter(listener));
    return true;
  }

  @Override
  public DirectChildSubscribeResult subscribeDirectChildChange(String key,
      DirectChildChangeListener listener, boolean skipWatchingNonExistNode) {
    ChildrenSubscribeResult result =
        _zkClient.subscribeChildChanges(key, new DirectChildListenerAdapter(listener), skipWatchingNonExistNode);
    return new DirectChildSubscribeResult(result.getChildren(), result.isInstalled());
  }

  @Override
  public boolean subscribeStateChanges(ConnectStateChangeListener listener) {
    _zkClient.subscribeStateChanges(new StateChangeListenerAdapter(listener));
    return true;
  }

  @Override
  public boolean subscribeChildChanges(String key, ChildChangeListener listener, boolean skipWatchingNonExistNode) {
    if (skipWatchingNonExistNode && exists(key) == null) {
      return false;
    }
    _zkClient.subscribePersistRecursiveListener(key, new ChildListenerAdapter(listener));
    return true;
  }

  @Override
  public void unsubscribeDataChange(String key, DataChangeListener listener) {
    _zkClient.unsubscribeDataChanges(key, new DataListenerAdapter(listener));
  }

  @Override
  public void unsubscribeDirectChildChange(String key, DirectChildChangeListener listener) {
    _zkClient.unsubscribeChildChanges(key, new DirectChildListenerAdapter(listener));
  }

  // TODO: add impl and remove UnimplementedException
  @Override
  public void unsubscribeChildChanges(String key, ChildChangeListener listener) {
    _zkClient.unsubscribePersistRecursiveListener(key, new ChildListenerAdapter(listener));
  }

  @Override
  public void unsubscribeConnectStateChanges(ConnectStateChangeListener listener) {
    _zkClient.subscribeStateChanges(new StateChangeListenerAdapter(listener));
  }

  @Override
  public boolean waitUntilExists(String key, TimeUnit timeUnit, long time) {
    return false;
  }

  @Override
  public boolean[] create(List<String> key, List<T> data, List<EntryMode> mode) {
    return new boolean[0];
  }

  @Override
  public boolean[] create(List<String> key, List<T> data) {
    return new boolean[0];
  }

  @Override
  public boolean[] delete(List<String> keys) {
    return new boolean[0];
  }

  @Override
  public List<Stat> exists(List<String> keys) {
    return null;
  }

  @Override
  public List<T> get(List<String> keys) {
    return null;
  }

  @Override
  public List<T> update(List<String> keys, List<DataUpdater<T>> updater) {
    return null;
  }

  @Override
  public boolean[] set(List<String> keys, List<T> datas, List<Integer> version) {
    return new boolean[0];
  }

  @Override
  public void close() {
    disconnect();
  }

  @Override
  public List<OpResult> transactionOP(Iterable<Op> ops) {
    // Convert list of MetaClient Ops to Zk Ops
    List<org.apache.zookeeper.Op> zkOps = ZkMetaClientUtil.metaClientOpsToZkOps(ops);
    // Execute Zk transactional support
    List<org.apache.zookeeper.OpResult> zkResult = _zkClient.multi(zkOps);
    // Convert list of Zk OpResults to MetaClient OpResults
    return ZkMetaClientUtil.zkOpResultToMetaClientOpResults(zkResult);
  }

  @Override
  public byte[] serialize(T data, String path) {
    return _zkClient.serialize(data, path);
  }

  @Override
  public T deserialize(byte[] bytes, String path) {
    return _zkClient.deserialize(bytes, path);
  }

  /**
   * A clean up method called when connect state change or MetaClient is closing.
   * @param cancel If we want to cancel the reconnect monitor thread.
   * @param close If we want to close ZkClient.
   */
  private void cleanUpAndClose(boolean cancel, boolean close) {
    _zkClientConnectionMutex.lock();
    try {
      if (close && !_zkClient.isClosed()) {
        _zkClient.close();
        // TODO: need to unsubscribe all persist watcher from ZK
        // Add this in ZkClient when persist watcher change is in
        // Also need to manually send CLOSED state change to state
        // change listener (in change adapter)
        LOG.info("ZkClient is closed");
      }

      if (cancel && _reconnectMonitorFuture != null) {
        _reconnectMonitorFuture.cancel(true);
        LOG.info("ZkClient reconnect monitor thread is canceled");
      }

    } finally {
      _zkClientConnectionMutex.unlock();
    }
  }

  /**
   * MetaClient uses Helix ZkClient (@see org.apache.helix.zookeeper.impl.client.ZkClient) to connect
   * to ZK. Current implementation of ZkClient auto-reconnects infinitely. We use monitor thread
   * in ZkMetaClient to monitor reconnect status and close ZkClient when the client still is in
   * disconnected state when it reach reconnect timeout.
   *
   *
   * case 1: Start the monitor thread when ZkMetaClient gets disconnected even to check connect state
   *         when timeout reached. If not re-connected when timed out, kill the monitor thread
   *         and close ZkClient.
   * [MetaClient thread]        ---------------------------------------------------------------
   *                              ( When disconnected, schedule a event
   *                              to check connect state after timeout)
   * [Reconnect monitor thread]          --------------------------------------
   *                                   ^                                     |  not reconnected when timed out
   *                                  /                                      |
   *                                 | disconnected event                    v
   * [ZkClient]               -------X---------------------------------------X zkClient.close()
   * [ZkClient exp back              |         X            X
   *  -off retry connection]         |--------|--------------|--------------
   *
   *
   * case 2: Start the monitor thread when ZkMetaClient gets disconnected even to check connect state
   *         when timeout reached. If re-connected before timed out, cancel the delayed monitor thread.
   *
   * [MetaClient thread]       ---------------------------------------------------------------
   *                            (cancel scheduled task when reconnected)
   * [Reconnect monitor]               ---------------------------------X
   *                                  ^                                ^
   *                                 /                                /
   *                                | disconnected event             |  reconnected event
   * [ZkClient]                -----X------------------------------------------------------
   * [ZkClient exp back             |        X                      Y  Reconnected before timed out
   *  -off retry connection]        |--------| ---------------------|
   *
   *
   * case 3: Start the monitor thread when ZkMetaClient gets disconnected even to check connect state
   *         when timeout reached. If re-connected errored, kill the monitor thread  and cancel the
   *         delayed monitor thread.
   * [MetaClient thread]       ---------------------------------------------------------------
   *                          (cancel scheduled task and close ZkClient when reconnected error)
   * [Reconnect monitor]              ----------------------------------X
   *                                 ^                               ^  |
   *                                /                           err /   |
   *                               | disconnected event            |    v close ZkClient
   * [ZkClient]               -----X-------------------------------X ---X
   * [ZkClient exp back            |        X                     ^ Reconnect error
   *  -off retry connection]       |--------| --------------------|
   *
   */

  private class ReconnectStateChangeListener implements IZkStateListener {
    // Schedule a monitor to track ZkClient auto reconnect when Disconnected
    // Cancel the monitor thread when connected.
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
      if (state == Watcher.Event.KeeperState.Disconnected) {                        // ------case 1
        // Expired. start a new event monitoring retry
        _zkClientConnectionMutex.lockInterruptibly();
        try {
          if (_reconnectMonitorFuture == null || _reconnectMonitorFuture.isCancelled()
              || _reconnectMonitorFuture.isDone()) {
            _reconnectMonitorFuture = _zkClientReconnectMonitor.schedule(() -> {
              if (!_zkClient.getConnection().getZookeeperState().isConnected()) {
                cleanUpAndClose(false, true);
              }
            }, _reconnectTimeout, TimeUnit.MILLISECONDS);
            LOG.info("ZkClient is Disconnected, schedule a reconnect monitor after {}",
                _reconnectTimeout);
          }
        } finally {
          _zkClientConnectionMutex.unlock();
        }
      } else if (state == Watcher.Event.KeeperState.SyncConnected
          || state == Watcher.Event.KeeperState.ConnectedReadOnly) {               // ------ case 2
        cleanUpAndClose(true, false);
        LOG.info("ZkClient is SyncConnected, reconnect monitor thread is canceled (if any)");
      }
    }

    // Cancel the monitor thread when connected.
    @Override
    public void handleNewSession(String sessionId) throws Exception {             // ------ case 2
      cleanUpAndClose(true, false);
      LOG.info("New session initiated in ZkClient, reconnect monitor thread is canceled (if any)");
    }

    // Cancel the monitor thread and close ZkClient when connect error.
    @Override
    public void handleSessionEstablishmentError(Throwable error) throws Exception {    // -- case 3
      cleanUpAndClose(true, true);
      LOG.info("New session initiated in ZkClient, reconnect monitor thread is canceled (if any)");
    }
  }

  @VisibleForTesting
  ZkClient getZkClient() {
    return _zkClient;
  }
}
