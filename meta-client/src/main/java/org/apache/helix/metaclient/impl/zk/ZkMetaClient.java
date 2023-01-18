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
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.apache.helix.metaclient.constants.MetaClientBadVersionException;
import org.apache.helix.metaclient.constants.MetaClientException;
import org.apache.helix.metaclient.constants.MetaClientInterruptException;
import org.apache.helix.metaclient.constants.MetaClientNoNodeException;
import org.apache.helix.metaclient.constants.MetaClientTimeoutException;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.EphemeralType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;


public class ZkMetaClient<T> implements MetaClientInterface<T>, AutoCloseable {

  private final ZkClient _zkClient;
  //Default ACL value until metaClient Op has ACL of its own.
  private final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
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
      _zkClient.create(key, data, metaClientModeToZkMode(mode));
    } catch (ZkException | KeeperException e) {
      throw new MetaClientException(e);
    }
  }

  private static CreateMode metaClientModeToZkMode(EntryMode mode) throws KeeperException {
    switch (mode) {
      case PERSISTENT:
        return CreateMode.PERSISTENT;
      case EPHEMERAL:
        return CreateMode.EPHEMERAL;
      default:
        return CreateMode.PERSISTENT;
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
  public T update( String key, DataUpdater<T> updater) {
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
      return new Stat(convertZkEntryMode(zkStats.getEphemeralOwner()), zkStats.getVersion());
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

  // Zookeeper execute async callbacks at zookeeper server side. In our first version of
  // implementation, we will keep this behavior.
  // In later version, we may consider creating a thread pool to execute registered callbacks.
  // However, this will change metaclient from stateless to stateful.
  @Override
  public void setAsyncExecPoolSize(int poolSize) {

  }

  @Override
  public void asyncCreate(String key, Object data, EntryMode mode, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncSet(String key, Object data, int version, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncUpdate(String key, DataUpdater updater, AsyncCallback.DataCallback cb) {

  }

  @Override
  public void asyncGet(String key, AsyncCallback.DataCallback cb) {

  }

  @Override
  public void asyncCountChildren(String key, AsyncCallback.DataCallback cb) {

  }

  @Override
  public void asyncExist(String key, AsyncCallback.StatCallback cb) {

  }

  @Override
  public void asyncDelete(String keys, AsyncCallback.VoidCallback cb) {

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
  public void asyncTransaction(Iterable iterable, AsyncCallback.TransactionCallback cb) {

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
  public boolean subscribeDataChange(String key, DataChangeListener listener,
      boolean skipWatchingNonExistNode, boolean persistListener) {
    return false;
  }

  @Override
  public DirectChildSubscribeResult subscribeDirectChildChange(String key,
      DirectChildChangeListener listener, boolean skipWatchingNonExistNode,
      boolean persistListener) {
    return null;
  }

  @Override
  public boolean subscribeStateChanges(ConnectStateChangeListener listener,
      boolean persistListener) {
    return false;
  }

  @Override
  public boolean subscribeChildChanges(String key, ChildChangeListener listener,
      boolean skipWatchingNonExistNode, boolean persistListener) {
    return false;
  }

  @Override
  public void unsubscribeDataChange(String key, DataChangeListener listener) {

  }

  @Override
  public void unsubscribeDirectChildChange(String key, DirectChildChangeListener listener) {

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

  /**
   * A converter class to transform {@link DataChangeListener} to {@link IZkDataListener}
   */
  static class DataListenerConverter implements IZkDataListener {
    private final DataChangeListener _listener;

    DataListenerConverter(DataChangeListener listener) {
      _listener = listener;
    }

    private DataChangeListener.ChangeType convertType(Watcher.Event.EventType eventType) {
      switch (eventType) {
        case NodeCreated: return DataChangeListener.ChangeType.ENTRY_CREATED;
        case NodeDataChanged: return DataChangeListener.ChangeType.ENTRY_UPDATE;
        case NodeDeleted: return DataChangeListener.ChangeType.ENTRY_DELETED;
        default: throw new IllegalArgumentException("EventType " + eventType + " is not supported.");
      }
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      throw new UnsupportedOperationException("handleDataChange(String dataPath, Object data) is not supported.");
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      handleDataChange(dataPath, null, Watcher.Event.EventType.NodeDeleted);
    }

    @Override
    public void handleDataChange(String dataPath, Object data, Watcher.Event.EventType eventType) throws Exception {
      _listener.handleDataChange(dataPath, data, convertType(eventType));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DataListenerConverter that = (DataListenerConverter) o;
      return _listener.equals(that._listener);
    }

    @Override
    public int hashCode() {
      return _listener.hashCode();
    }
  }

  private static MetaClientException translateZkExceptionToMetaclientException(ZkException e) {
    if (e instanceof ZkNodeExistsException) {
      return new MetaClientNoNodeException(e);
    } else if (e instanceof ZkBadVersionException) {
      return new MetaClientBadVersionException(e);
    } else if (e instanceof ZkTimeoutException) {
      return new MetaClientTimeoutException(e);
    } else if (e instanceof ZkInterruptedException) {
      return new MetaClientInterruptException(e);
    } else {
      return new MetaClientException(e);
    }
  }

  private static EntryMode convertZkEntryMode(long ephemeralOwner) {
    EphemeralType zkEphemeralType = EphemeralType.get(ephemeralOwner);
    switch (zkEphemeralType) {
      case VOID:
        return EntryMode.PERSISTENT;
      case CONTAINER:
        return EntryMode.CONTAINER;
      case NORMAL:
        return EntryMode.EPHEMERAL;
      // TODO: TTL is not supported now.
      //case TTL:
      //  return EntryMode.TTL;
      default:
        throw new IllegalArgumentException(zkEphemeralType + " is not supported.");
    }
  }


  @Override
  public List<OpResult> transactionOP(Iterable<Op> iterable) throws KeeperException {
    // Convert list of MetaClient Ops to Zk Ops
    List<org.apache.zookeeper.Op> zkOps = metaClientOpToZk(iterable);
    // Execute Zk transactional support
    List<org.apache.zookeeper.OpResult> zkResult = _zkClient.multi(zkOps);
    // Convert list of Zk OpResults to MetaClient OpResults
    return zkOpResultToMetaClient(zkResult);
  }

  /**
   * Helper function for transactionOp. Converts MetaClient Op's into Zk Ops to execute
   * zk transactional support.
   * @param ops
   * @return
   */
  public List<org.apache.zookeeper.Op> metaClientOpToZk(Iterable<Op> ops) throws KeeperException {
    List<org.apache.zookeeper.Op> zkOps = new ArrayList<>();
    org.apache.zookeeper.Op temp;
    for (Op op : ops) {
      switch (op.getType()) {
        case CREATE:
          int zkFlag = zkFlagFromEntryMode(((Op.Create) op).getEntryMode());
          temp = org.apache.zookeeper.Op.create(
              op.getPath(), ((Op.Create) op).getData(), DEFAULT_ACL, CreateMode.fromFlag(zkFlag));
          break;
        case DELETE:
          temp = org.apache.zookeeper.Op.delete(
              op.getPath(), ((Op.Delete) op).getVersion());
          break;
        case SET:
          temp = org.apache.zookeeper.Op.setData(
              op.getPath(), ((Op.Set) op).getData(), ((Op.Set) op).getVersion());
          break;
        case CHECK:
          temp = org.apache.zookeeper.Op.check(
              op.getPath(), ((Op.Check) op).getVersion());
          break;
        default:
          throw new KeeperException.BadArgumentsException();
      }
      zkOps.add(temp);
    }
    return zkOps;
  }

  /**
   * Helper function for transactionOP. Converts the result from calling zk transactional support into
   * metaclient OpResults.
   * @param zkResult
   * @return
   */
  public List<OpResult> zkOpResultToMetaClient(List<org.apache.zookeeper.OpResult> zkResult) throws KeeperException.BadArgumentsException {
    List<OpResult> metaClientOpResult = new ArrayList<>();
    OpResult temp;
    EntryMode zkMode;
    for (org.apache.zookeeper.OpResult opResult : zkResult) {
      Stat metaClientStat;
      switch (opResult.getType()) {
        // CreateResult
        case 1:
          temp = new OpResult.CreateResult(
                  ((org.apache.zookeeper.OpResult.CreateResult) opResult).getPath());
          break;
        // DeleteResult
        case 2:
          temp = new OpResult.DeleteResult();
          break;
        // GetDataResult
        case 4:
          org.apache.zookeeper.OpResult.GetDataResult zkOpGetDataResult =
                  (org.apache.zookeeper.OpResult.GetDataResult) opResult;
          metaClientStat = new Stat(convertZkEntryMode(zkOpGetDataResult.getStat().getEphemeralOwner()),
              zkOpGetDataResult.getStat().getVersion());
          temp = new OpResult.GetDataResult(zkOpGetDataResult.getData(), metaClientStat);
          break;
        //SetDataResult
        case 5:
          org.apache.zookeeper.OpResult.SetDataResult zkOpSetDataResult =
                  (org.apache.zookeeper.OpResult.SetDataResult) opResult;
          metaClientStat = new Stat(convertZkEntryMode(zkOpSetDataResult.getStat().getEphemeralOwner()),
              zkOpSetDataResult.getStat().getVersion());
          temp = new OpResult.SetDataResult(metaClientStat);
          break;
        //GetChildrenResult
        case 8:
          temp = new OpResult.GetChildrenResult(
                  ((org.apache.zookeeper.OpResult.GetChildrenResult) opResult).getChildren());
          break;
        //CheckResult
        case 13:
          temp = new OpResult.CheckResult();
          break;
        //CreateResult with stat
        case 15:
          org.apache.zookeeper.OpResult.CreateResult zkOpCreateResult =
                  (org.apache.zookeeper.OpResult.CreateResult) opResult;
          metaClientStat = new Stat(convertZkEntryMode(zkOpCreateResult.getStat().getEphemeralOwner()),
              zkOpCreateResult.getStat().getVersion());
          temp = new OpResult.CreateResult(zkOpCreateResult.getPath(), metaClientStat);
          break;
        //ErrorResult
        case -1:
          temp = new OpResult.ErrorResult(
                  ((org.apache.zookeeper.OpResult.ErrorResult) opResult).getErr());
          break;
        default:
          throw new KeeperException.BadArgumentsException();
      }
      metaClientOpResult.add(temp);
    }
    return metaClientOpResult;
  }

  private int zkFlagFromEntryMode (EntryMode entryMode) {
    String mode = entryMode.name();
    if (mode.equals(EntryMode.PERSISTENT.name())) {
      return 0;
    }
    if (mode.equals(EntryMode.EPHEMERAL.name())) {
      return 1;
    }
    return -1;
  }
}
