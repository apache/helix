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

import java.io.Closeable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
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
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.ZkEventThread;
import org.apache.helix.zookeeper.zkclient.ZkLock;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkMetaClient<T> implements MetaClientInterface<T>, Watcher, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClient.class);
  private static final AtomicLong UID = new AtomicLong(0);
  private static final Set<Event.EventType> DATA_CHANGE_EVENT_TYPE = EnumSet.of(
      Event.EventType.NodeCreated, Event.EventType.NodeDeleted, Event.EventType.NodeDataChanged);
  private final long _uid;
  private final Map<String, Set<DataChangeListener>> _dataChangeListener = new HashMap<>();
  private final Map<String, Set<DataChangeListener>> _oneTimeDataChangeListener = new HashMap<>();
  private final ZkClient _zkClient;
  private final ZkMetaClientConfig _config;
  private final ReentrantReadWriteLock _readWriteLock = new ReentrantReadWriteLock();

  private ZkEventThread _eventThread;

  private boolean _shutdownTriggered;

  public ZkMetaClient(ZkMetaClientConfig config) {
    _uid = UID.getAndIncrement();
    _config = config;
    _eventThread = new ZkEventThread(config.getConnectionAddress());
    _zkClient = new ZkClient.Builder()
        .setConnection(new ZkConnection(config.getConnectionAddress(),
            (int) config.getSessionTimeoutInMillis()))
        .setConnectionTimeout((int) config.getConnectionInitTimeoutInMillis())
        .setOperationRetryTimeout(-1L)
        .setZkSerializer(config.getZkSerializer())
        .setMonitorType(config.getMonitorType())
        .setMonitorKey(config.getMonitorKey())
        .setMonitorInstanceName(config.getMonitorInstanceName())
        .setMonitorRootPathOnly(config.getMonitorRootPathOnly())
        .setWatcher(this) // this ZkMetaClient will be used as the only Watcher impl
        .setConnectOnInit(false)
        .build();
  }

  @Override
  public void create(String key, T data) {
    // TODO: impl
    _zkClient.createPersistent(key, true);
  }

  @Override
  public void create(String key, T data, EntryMode mode) {
    switch (mode) {
      case PERSISTENT: _zkClient.create(key, data, CreateMode.PERSISTENT);
      case EPHEMERAL: _zkClient.create(key, data, CreateMode.EPHEMERAL);
      case CONTAINER: _zkClient.create(key, data, CreateMode.CONTAINER);
      default: throw new IllegalArgumentException("EntryMode " + mode + " is not supported.");
    }
  }

  @Override
  public void set(String key, T data, int version) {
    // TODO: impl
    _zkClient.writeData(key, data);
  }

  @Override
  public T update(String key, DataUpdater<T> updater) {
    return null;
  }

  @Override
  public Stat exists(String key) {
    return null;
  }

  @Override
  public T get(String key) {
    return null;
  }

  @Override
  public List<String> getDirestChildrenKeys(String key) {
    return null;
  }

  @Override
  public int countDirestChildren(String key) {
    return 0;
  }

  @Override
  public boolean delete(String key) {
    // TODO: impl
    return _zkClient.delete(key);
  }

  @Override
  public boolean recursiveDelete(String key) {
    return false;
  }

  @Override
  public void setAsyncExecPoolSize(int poolSize) {

  }

  @Override
  public void asyncCreate(String key, T data, EntryMode mode, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncSet(String key, T data, int version, AsyncCallback.VoidCallback cb) {

  }

  @Override
  public void asyncUpdate(String key, DataUpdater<T> updater, AsyncCallback.DataCallback cb) {

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
  public boolean[] create(List<String> key, List<T> data, List<EntryMode> mode) {
    return new boolean[0];
  }

  @Override
  public boolean[] create(List<String> key, List<T> data) {
    return new boolean[0];
  }

  @Override
  public void asyncTransaction(Iterable<Op> iterable, AsyncCallback.TransactionCallback cb) {

  }

  @Override
  public boolean connect() {
    _zkClient.connect(_config.getConnectionInitTimeoutInMillis(), this);
    _eventThread = new ZkEventThread(_zkClient.getConnection().getServers());
    _eventThread.start();
    return true;
  }

  @Override
  public void disconnect() {
    close();
  }

  @Override
  public void close() {
    setShutdownTrigger(true);
    _zkClient.close();
    try {
      _eventThread.interrupt();
      _eventThread.join(2000);
    } catch (InterruptedException e) {
      //TODO: to remove
      System.out.println("Caught exception in close(), " + e);
    }
  }

  @Override
  public ConnectState getClientConnectionState() {
    return null;
  }

  @Override
  public boolean subscribeDataChange(String key, DataChangeListener listener,
      boolean skipWatchingNonExistNode, boolean persistListener) {
    _readWriteLock.writeLock().lock();
    try {
      if (!persistListener) {
        Set<DataChangeListener> entryListeners = _oneTimeDataChangeListener.computeIfAbsent(key, k -> new HashSet<>());
        entryListeners.add(listener);
        _oneTimeDataChangeListener.put(key, entryListeners);
      } else {
        Set<DataChangeListener> entryListeners = _dataChangeListener.computeIfAbsent(key, k -> new HashSet<>());
        entryListeners.add(listener);
        _dataChangeListener.put(key, entryListeners);
      }
    } finally {
      _readWriteLock.writeLock().unlock();
    }
    // TODO: fix persistent watcher leakage!!!
    boolean watchInstalled = _zkClient.watchForData(key, skipWatchingNonExistNode, persistListener);
    if (!watchInstalled) {
      unsubscribeDataChange(key, listener);
      LOG.error("ZkMetaClient {} failed to subscribe data change on key {}", _uid, key);
      return false;
    }
    return true;
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
    _readWriteLock.writeLock().lock();
    try {
      removeFromListenerMap(key, listener, _oneTimeDataChangeListener);
      removeFromListenerMap(key, listener, _dataChangeListener);
    } finally {
      _readWriteLock.writeLock().unlock();
    }
  }

  private static void removeFromListenerMap(String key, DataChangeListener listener,
      Map<String, Set<DataChangeListener>> listenerMap) {
    Set<DataChangeListener> listeners = listenerMap.get(key);
    if (listeners == null) {
      return;
    }
    listeners.remove(listener);
    if (listeners.isEmpty()) {
      listenerMap.remove(key);
    }
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
  public List<OpResult> transactionOP(Iterable<Op> iterable) {
    return null;
  }

  @Override
  public void process(WatchedEvent event) {
    long notificationTime = System.currentTimeMillis();
    LOG.debug("ZkMetaClient {} receives event: {} ", _uid, event);

    // local _eventThread
    if (_shutdownTriggered) {
      LOG.debug("ZkMetaClient {} Ignoring event {} because shutdown has been triggered.", _uid, event);
      return;
    }
    getZkEventLock().lock();
    try {
      handleStateChanged(event);
      handleDataChanged(event, notificationTime);
    } finally {
      signalConditions(event);
      getZkEventLock().unlock();
    }

  }

  private void signalConditions(WatchedEvent event) {
    boolean stateChanged = event.getType() == Event.EventType.None;
    boolean znodeChanged = event.getPath() != null;
    boolean dataChanged = event.getType() == Event.EventType.NodeDataChanged
        || event.getType() == Event.EventType.NodeDeleted
        || event.getType() == Event.EventType.NodeCreated
        || event.getType() == Event.EventType.NodeChildrenChanged;
    if (stateChanged) {
      getZkEventLock().getStateChangedCondition().signalAll();
      if (event.getState() == Event.KeeperState.Expired) {
        getZkEventLock().getZNodeEventCondition().signalAll();
        getZkEventLock().getDataChangedCondition().signalAll();
      }
    }
    if (znodeChanged) {
      getZkEventLock().getZNodeEventCondition().signalAll();
    }
    if (dataChanged) {
      getZkEventLock().getDataChangedCondition().signalAll();
    }
  }

  private void handleDataChanged(WatchedEvent event, long notificationTime) {
    if (_zkClient.getMonitor() != null) {
      _zkClient.getMonitor().increaseDataChangeEventCounter();
    }
    if (!DATA_CHANGE_EVENT_TYPE.contains(event.getType())) {
      return;
    }
    // TODO: to remove
    System.out.println("handleDataChanged triggered by " + event);
    Set<DataChangeListener> onetimeListeners;
    _readWriteLock.readLock().lock();
    try {
      Set<DataChangeListener> listeners = _dataChangeListener.getOrDefault(event.getPath(), Collections.emptySet());
      onetimeListeners = _oneTimeDataChangeListener.getOrDefault(event.getPath(), Collections.emptySet());
      if (listeners.isEmpty() && onetimeListeners.isEmpty()) {
        return;
      }
      try {
        final ZkPathStatRecord pathStatRecord = new ZkPathStatRecord(event.getPath(), _zkClient.getMonitor());
        Stream.concat(listeners.stream(), onetimeListeners.stream())
            .forEach(listener ->
                _eventThread.send(new DataChangedZkEvent(event, listener, pathStatRecord, notificationTime)));
      } catch (Exception e) {
        LOG.error("ZkMetaClient {} failed to process event {}.", _uid, event, e);
      }
    } finally {
      _readWriteLock.readLock().unlock();
    }
    if (onetimeListeners.isEmpty()) {
      return;
    }
    // remove the one-time listeners
    _readWriteLock.writeLock().lock();
    try {
      _oneTimeDataChangeListener.get(event.getPath()).removeAll(onetimeListeners);
    } finally {
      _readWriteLock.writeLock().unlock();
    }
  }

  private void handleStateChanged(WatchedEvent event) {
    if (event.getType() != Event.EventType.None) {
      // not state change
      return;
    }
    // TODO: to remove
    System.out.println("handleStateChanged triggered by " + event);
    _zkClient.setCurrentState(event.getState());
    if (_zkClient.getMonitor() != null) {
      _zkClient.getMonitor().increaseStateChangeEventCounter();
      if (event.getState() == Event.KeeperState.Expired) {
        _zkClient.getMonitor().increaseExpiredSessionCounter();
      }
    }
    // TODO: impl handle state changed event
  }

  protected ZkLock getZkEventLock() {
    return _zkClient.getEventLock();
  }

  public void setShutdownTrigger(boolean triggerState) {
    _shutdownTriggered = triggerState;
  }

  public boolean getShutdownTrigger() {
    return _shutdownTriggered;
  }

  private static class ZkPathStatRecord {
    private final String _path;
    private final ZkClientMonitor _monitor;
    private org.apache.zookeeper.data.Stat _stat = null;
    private boolean _checked = false;

    public ZkPathStatRecord(String path, ZkClientMonitor monitor) {
      _path = path;
      _monitor = monitor;
    }

    public boolean pathExists() {
      return _stat != null;
    }

    public boolean pathChecked() {
      return _checked;
    }

    /*
     * Note this method is not thread safe.
     */
    public void recordPathStat(org.apache.zookeeper.data.Stat stat, long notificationTime) {
      recordPathStat(stat);
      if (_monitor != null && stat != null) {
        long updateTime = Math.max(stat.getCtime(), stat.getMtime());
        if (notificationTime > updateTime) {
          _monitor.recordDataPropagationLatency(_path, notificationTime - updateTime);
        } // else, the node was updated again after the notification. Propagation latency is
        // unavailable.
      }
    }

    public void recordPathStat(org.apache.zookeeper.data.Stat stat) {
      _checked = true;
      _stat = stat;
    }
  }

  private class DataChangedZkEvent extends ZkEventThread.ZkEvent {

    private final WatchedEvent _event;
    private final DataChangeListener _listener;
    private final ZkPathStatRecord _pathStatRecord;
    private final long _notificationTime;

    public DataChangedZkEvent(WatchedEvent event, DataChangeListener listener,
        ZkPathStatRecord pathStatRecord, long notificationTime) {
      super("Data of " + event.getPath() + " sent to " + listener);
      _event = event;
      _listener = listener;
      _pathStatRecord = pathStatRecord;
      _notificationTime = notificationTime;
    }

    @Override
    public void run() throws Exception {
      String path = _event.getPath();
      if (!_pathStatRecord.pathChecked()) {
        org.apache.zookeeper.data.Stat stat;
        if (_event.getType() == Event.EventType.NodeDeleted) {
          stat = _zkClient.getStat(path);
        } else {
          stat = _zkClient.installWatchOnlyPathExist(path);
        }
        _pathStatRecord.recordPathStat(stat, _notificationTime);
      }
      if (!_pathStatRecord.pathExists()) {
        _listener.handleDataChange(path, null, DataChangeListener.ChangeType.ENTRY_DELETED);
      } else {
        Object data;
        try {
          // TODO: the data is redundantly read multiple times when multiple listeners exist
          data = get(Collections.singletonList(path));
        } catch (ZkNoNodeException e) {
          LOG.error("ZkMetaClient {} failed to read data for path: {}.", _uid, path, e);
          _listener.handleDataChange(path, null, DataChangeListener.ChangeType.ENTRY_DELETED);
          return;
        }
        if (_event.getType() == Event.EventType.NodeCreated) {
          _listener.handleDataChange(path, data, DataChangeListener.ChangeType.ENTRY_CREATED);
        } else if (_event.getType() == Event.EventType.NodeDataChanged) {
          _listener.handleDataChange(path, data, DataChangeListener.ChangeType.ENTRY_UPDATE);
        }
      }
    }
  }
}
