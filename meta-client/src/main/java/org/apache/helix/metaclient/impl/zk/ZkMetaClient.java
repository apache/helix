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
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.NotImplementedException;
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
import org.apache.helix.metaclient.impl.zk.adapter.DataListenerAdapter;
import org.apache.helix.metaclient.impl.zk.adapter.DirectChildListenerAdapter;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientCreateCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientDeleteCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientExistCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientGetCallbackHandler;
import org.apache.helix.metaclient.impl.zk.adapter.ZkMetaClientSetCallbackHandler;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil;
import org.apache.helix.zookeeper.api.client.ChildrenSubscribeResult;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.EphemeralType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil.convertZkEntryModeToMetaClientEntryMode;
import static org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil.translateZkExceptionToMetaclientException;

public class ZkMetaClient<T> implements MetaClientInterface<T>, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClient.class);
  private final ZkClient _zkClient;
  private final int _connectionTimeout;

  public ZkMetaClient(ZkMetaClientConfig config) {
    _connectionTimeout = (int) config.getConnectionInitTimeoutInMillis();
    _zkClient = new ZkClient(
        new ZkConnection(config.getConnectionAddress(), (int) config.getSessionTimeoutInMillis()),
        _connectionTimeout, -1 /*operationRetryTimeout*/, config.getZkSerializer(),
        config.getMonitorType(), config.getMonitorKey(), config.getMonitorInstanceName(),
        config.getMonitorRootPathOnly(), false);
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

  @Override
  public Stat exists(String key) {
    org.apache.zookeeper.data.Stat zkStats;
    try {
      zkStats = _zkClient.getStat(key);
      if (zkStats == null) {
        return null;
      }
      return new Stat(convertZkEntryModeToMetaClientEntryMode(zkStats.getEphemeralOwner()),
          zkStats.getVersion());
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public T get(String key) {
    return _zkClient.readData(key, true);
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
  // callbacks executed in ZkClient event thread, and reuse zkclient retry logic.

  // It is highly recommended NOT to perform any blocking operation inside the callbacks.
  // If you block the thread the meta client won't process other events.

  // corresponding callbacks for each operation are invoked in order.
  @Override
  public void setAsyncExecPoolSize(int poolSize) {
    throw new UnsupportedOperationException(
        "All async calls are executed at the current client thread.");
  }

  @Override
  public void asyncCreate(String key, Object data, EntryMode mode, AsyncCallback.VoidCallback cb) {
    try {
      _zkClient.asyncCreate(key, data, ZkMetaClientUtil.convertMetaClientMode(mode),
          new ZkMetaClientCreateCallbackHandler(cb));
    } catch (KeeperException e) {
      throw translateZkExceptionToMetaclientException(ZkException.create(e));
    }
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

  }

  @Override
  public void asyncSet(String key, T data, int version, AsyncCallback.StatCallback cb) {
    _zkClient.asyncSetData(key, data, version,
        new ZkMetaClientSetCallbackHandler(cb));
  }

  @Override
  public void connect() {
    // TODO: throws IllegalStateException when already connected
    try {
      _zkClient.connect(_connectionTimeout, _zkClient);
    } catch (ZkException e) {
      throw translateZkExceptionToMetaclientException(e);
    }
  }

  @Override
  public void disconnect() {
    // TODO: This is a temp impl for test only. no proper interrupt handling and error handling.
    _zkClient.close();
  }

  @Override
  public ConnectState getClientConnectionState() {
    return null;
  }

  @Override
  public boolean subscribeDataChange(String key, DataChangeListener listener, boolean skipWatchingNonExistNode) {
    _zkClient.subscribeDataChanges(key, new DataListenerAdapter(listener));
    return false;
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
    return false;
  }

  @Override
  public boolean subscribeChildChanges(String key, ChildChangeListener listener, boolean skipWatchingNonExistNode) {
    return false;
  }

  @Override
  public void unsubscribeDataChange(String key, DataChangeListener listener) {
    _zkClient.unsubscribeDataChanges(key, new DataListenerAdapter(listener));
  }

  @Override
  public void unsubscribeDirectChildChange(String key, DirectChildChangeListener listener) {
    _zkClient.unsubscribeChildChanges(key, new DirectChildListenerAdapter(listener));
  }

  @Override
  public void unsubscribeChildChanges(String key, ChildChangeListener listener) {

  }

  @Override
  public void unsubscribeConnectStateChanges(ConnectStateChangeListener listener) {

  }

  @Override
  public boolean waitUntilExists(String key, TimeUnit timeUnit, long time) {
    return false;
  }

  @Override
  public boolean[] create(List key, List data, List mode) {
    return new boolean[0];
  }

  @Override
  public boolean[] create(List key, List data) {
    return new boolean[0];
  }

  @Override
  public boolean[] delete(List keys) {
    return new boolean[0];
  }

  @Override
  public List<Stat> exists(List keys) {
    return null;
  }

  @Override
  public List get(List keys) {
    return null;
  }

  @Override
  public List update(List keys, List updater) {
    return null;
  }

  @Override
  public boolean[] set(List keys, List values, List version) {
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

  public T deserialize(byte[] bytes, String path) {
    return _zkClient.deserialize(bytes, path);
  }
}
