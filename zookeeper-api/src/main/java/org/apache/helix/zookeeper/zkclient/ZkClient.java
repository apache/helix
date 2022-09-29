package org.apache.helix.zookeeper.zkclient;

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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.JMException;
import org.apache.helix.zookeeper.api.client.ChildrenSubscribeResult;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.SessionAwareZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.util.ZNRecordUtil;
import org.apache.helix.zookeeper.zkclient.annotation.PreFetchChangedData;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallMonitorContext;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncRetryCallContext;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncRetryThread;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.zookeeper.zkclient.exception.ZkSessionMismatchedException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.helix.zookeeper.zkclient.util.ExponentialBackoffStrategy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Native ZkClient": not to be used directly.
 *
 * Abstracts the interaction with zookeeper and allows permanent (not just one time) watches on
 * nodes in ZooKeeper.
 * WARN: Do not use this class directly, use {@link org.apache.helix.zookeeper.impl.client.ZkClient} instead.
 */
public class ZkClient implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  public static final long TTL_NOT_SET = -1L;
  private static final long MAX_RECONNECT_INTERVAL_MS = 30000; // 30 seconds

  // If number of children exceeds this limit, getChildren() should not retry on connection loss.
  // This is a workaround for exiting retry on connection loss because of large number of children.
  // 100K is specific for helix messages which use UUID, making packet length just below 4 MB.
  // TODO: remove it once we have a better way to exit retry for this case
  private static final int NUM_CHILDREN_LIMIT = 100 * 1000;

  private static final boolean SYNC_ON_SESSION = Boolean.parseBoolean(
      System.getProperty(ZkSystemPropertyKeys.ZK_AUTOSYNC_ENABLED, "true"));
  private static final String SYNC_PATH = "/";

  private static AtomicLong UID = new AtomicLong(0);
  public final long _uid;

  // ZNode write size limit in bytes.
  // TODO: use ZKConfig#JUTE_MAXBUFFER once bumping up ZK to 3.5.2+
  private static final int WRITE_SIZE_LIMIT =
      Integer.getInteger(ZkSystemPropertyKeys.JUTE_MAXBUFFER, ZNRecord.SIZE_LIMIT);

  private final IZkConnection _connection;
  private final long _operationRetryTimeoutInMillis;
  private final Map<String, Set<IZkChildListener>> _childListener = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<IZkDataListenerEntry>> _dataListener =
      new ConcurrentHashMap<>();
  private final Set<IZkStateListener> _stateListener = new CopyOnWriteArraySet<>();
  private KeeperState _currentState;
  private final ZkLock _zkEventLock = new ZkLock();

  // When a new zookeeper instance is created in reconnect, its session id is not yet valid before
  // the zookeeper session is established(SyncConnected). To avoid session race condition in
  // handling new session, the new session event is only fired after SyncConnected. Meanwhile,
  // SyncConnected state is also received when re-opening the zk connection. So to avoid firing
  // new session event more than once, this flag is used to check.
  // It is set to false when once existing expires. And set it to true once the new session event
  // is fired the first time.
  private boolean _isNewSessionEventFired;

  private boolean _shutdownTriggered;
  private ZkEventThread _eventThread;
  // TODO PVo remove this later
  private Thread _zookeeperEventThread;
  private volatile boolean _closed;
  private PathBasedZkSerializer _pathBasedZkSerializer;
  private ZkClientMonitor _monitor;

  // To automatically retry the async operation, we need a separate thread other than the
  // ZkEventThread. Otherwise the retry request might block the normal event processing.
  protected final ZkAsyncRetryThread _asyncCallRetryThread;

  private class IZkDataListenerEntry {
    final IZkDataListener _dataListener;
    final boolean _prefetchData;

    public IZkDataListenerEntry(IZkDataListener dataListener, boolean prefetchData) {
      _dataListener = dataListener;
      _prefetchData = prefetchData;
    }

    public IZkDataListenerEntry(IZkDataListener dataListener) {
      _dataListener = dataListener;
      _prefetchData = false;
    }

    public IZkDataListener getDataListener() {
      return _dataListener;
    }

    public boolean isPrefetchData() {
      return _prefetchData;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IZkDataListenerEntry)) {
        return false;
      }

      IZkDataListenerEntry that = (IZkDataListenerEntry) o;

      return _dataListener.equals(that._dataListener);
    }

    @Override
    public int hashCode() {
      return _dataListener.hashCode();
    }
  }

  private class ZkPathStatRecord {
    private final String _path;
    private Stat _stat = null;
    private boolean _checked = false;

    public ZkPathStatRecord(String path) {
      _path = path;
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
    public void recordPathStat(Stat stat, OptionalLong notificationTime) {
      _checked = true;
      _stat = stat;

      if (_monitor != null && stat != null && notificationTime.isPresent()) {
        long updateTime = Math.max(stat.getCtime(), stat.getMtime());
        if (notificationTime.getAsLong() > updateTime) {
          _monitor.recordDataPropagationLatency(_path, notificationTime.getAsLong() - updateTime);
        } // else, the node was updated again after the notification. Propagation latency is
        // unavailable.
      }
    }
  }

  protected ZkClient(IZkConnection zkConnection, int connectionTimeout, long operationRetryTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      String monitorInstanceName, boolean monitorRootPathOnly) {
    if (zkConnection == null) {
      throw new NullPointerException("Zookeeper connection is null!");
    }

    _uid = UID.getAndIncrement();
    validateWriteSizeLimitConfig();

    _connection = zkConnection;
    _pathBasedZkSerializer = zkSerializer;
    _operationRetryTimeoutInMillis = operationRetryTimeout;
    _isNewSessionEventFired = false;

    _asyncCallRetryThread = new ZkAsyncRetryThread(zkConnection.getServers());
    _asyncCallRetryThread.start();
    LOG.debug("ZkClient created with uid {}, _asyncCallRetryThread id {}", _uid, _asyncCallRetryThread.getId());

    if (monitorKey != null && !monitorKey.isEmpty() && monitorType != null && !monitorType
        .isEmpty()) {
      _monitor =
          new ZkClientMonitor(monitorType, monitorKey, monitorInstanceName, monitorRootPathOnly,
              _eventThread);
    } else {
      LOG.info("ZkClient monitor key or type is not provided. Skip monitoring.");
    }

    connect(connectionTimeout, this);

    try {
      if (_monitor != null) {
        _monitor.register();
      }
    } catch (JMException e){
      LOG.error("Error in creating ZkClientMonitor", e);
    }
  }

  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    ChildrenSubscribeResult result = subscribeChildChanges(path, listener, false);
    return result.getChildren();
  }

  public ChildrenSubscribeResult subscribeChildChanges(String path, IZkChildListener listener, boolean skipWatchingNonExistNode) {
    synchronized (_childListener) {
      Set<IZkChildListener> listeners = _childListener.get(path);
      if (listeners == null) {
        listeners = new CopyOnWriteArraySet<>();
        _childListener.put(path, listeners);
      }
      listeners.add(listener);
    }

    List<String> children = watchForChilds(path, skipWatchingNonExistNode);
    if (children == null && skipWatchingNonExistNode) {
      unsubscribeChildChanges(path, listener);
      LOG.info("zkclient{}, watchForChilds failed to install no-existing watch and add listener. Path: {}", _uid, path);
      return new ChildrenSubscribeResult(children, false);
    }

    return new ChildrenSubscribeResult(children, true);
  }

  public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
    synchronized (_childListener) {
      final Set<IZkChildListener> listeners = _childListener.get(path);
      if (listeners != null) {
        listeners.remove(childListener);
      }
    }
  }

  public boolean subscribeDataChanges(String path, IZkDataListener listener, boolean skipWatchingNonExistNode) {
    Set<IZkDataListenerEntry> listenerEntries;
    synchronized (_dataListener) {
      listenerEntries = _dataListener.get(path);
      if (listenerEntries == null) {
        listenerEntries = new CopyOnWriteArraySet<>();
        _dataListener.put(path, listenerEntries);
      }

      boolean prefetchEnabled = isPrefetchEnabled(listener);
      IZkDataListenerEntry listenerEntry = new IZkDataListenerEntry(listener, prefetchEnabled);
      listenerEntries.add(listenerEntry);
      if (prefetchEnabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("zkclient {} subscribed data changes for {}, listener {}, prefetch data {}",
              _uid, path, listener, prefetchEnabled);
        }
      }
    }

    boolean watchInstalled = watchForData(path, skipWatchingNonExistNode);
    if (!watchInstalled) {
      // Now let us remove this handler.
      unsubscribeDataChanges(path, listener);
      LOG.info("zkclient {} watchForData failed to install no-existing path and thus add listener. Path: {}",
          _uid, path);
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("zkclient {}, Subscribed data changes for {}", _uid, path);
    }
    return true;
  }

   /**
    * Subscribe the path and the listener will handle data events of the path
    * WARNING: if the path is created after deletion, users need to re-subscribe the path
    * @param path The zookeeper path
    * @param listener Instance of {@link IZkDataListener}
    */
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    subscribeDataChanges(path, listener, false);
  }

  private boolean isPrefetchEnabled(IZkDataListener dataListener) {
    PreFetchChangedData preFetch = dataListener.getClass().getAnnotation(PreFetchChangedData.class);
    if (preFetch != null) {
      return preFetch.enabled();
    }

    Method callbackMethod = IZkDataListener.class.getMethods()[0];
    try {
      Method method = dataListener.getClass()
          .getMethod(callbackMethod.getName(), callbackMethod.getParameterTypes());
      PreFetchChangedData preFetchInMethod = method.getAnnotation(PreFetchChangedData.class);
      if (preFetchInMethod != null) {
        return preFetchInMethod.enabled();
      }
    } catch (NoSuchMethodException e) {
      LOG.warn("Zkclient {}, No method {} defined in listener {}",
          _uid, callbackMethod.getName(), dataListener.getClass().getCanonicalName());
    }

    return true;
  }

  public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
    synchronized (_dataListener) {
      final Set<IZkDataListenerEntry> listeners = _dataListener.get(path);
      if (listeners != null) {
        IZkDataListenerEntry listenerEntry = new IZkDataListenerEntry(dataListener);
        listeners.remove(listenerEntry);
      }
      if (listeners == null || listeners.isEmpty()) {
        _dataListener.remove(path);
      }
    }
  }

  public void subscribeStateChanges(final IZkStateListener listener) {
    synchronized (_stateListener) {
      _stateListener.add(listener);
    }
  }

  /**
   * Subscribes state changes for a {@link IZkStateListener} listener.
   *
   * @deprecated
   * This is deprecated. It is kept for backwards compatibility. Please use
   * {@link #subscribeStateChanges(IZkStateListener)}.
   *
   * @param listener {@link IZkStateListener} listener
   */
  @Deprecated
  public void subscribeStateChanges(
      final org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener) {
    subscribeStateChanges(new IZkStateListenerI0ItecImpl(listener));
  }

  public void unsubscribeStateChanges(IZkStateListener stateListener) {
    synchronized (_stateListener) {
      _stateListener.remove(stateListener);
    }
  }

  /**
   * Unsubscribes state changes for a {@link IZkStateListener} listener.
   *
   * @deprecated
   * This is deprecated. It is kept for backwards compatibility. Please use
   * {@link #unsubscribeStateChanges(IZkStateListener)}.
   *
   * @param stateListener {@link IZkStateListener} listener
   */
  @Deprecated
  public void unsubscribeStateChanges(
      org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener stateListener) {
    unsubscribeStateChanges(new IZkStateListenerI0ItecImpl(stateListener));
  }

  public void unsubscribeAll() {
    synchronized (_childListener) {
      _childListener.clear();
    }
    synchronized (_dataListener) {
      _dataListener.clear();
    }
    synchronized (_stateListener) {
      _stateListener.clear();
    }
  }

  // </listeners>

  /**
   * Create a persistent node.
   * @param path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistent(String path)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createPersistent(path, false);
  }

  /**
   * Create a persistent node with TTL.
   * @param path the path where you want the node to be created
   * @param ttl TTL of the node in milliseconds
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistentWithTTL(String path, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createPersistentWithTTL(path, false, ttl);
  }

  /**
   * Create a container node.
   * @param path the path where you want the node to be created
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createContainer(String path)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createContainer(path, false);
  }

  /**
   * Create a persistent node and set its ACLs.
   * @param path
   * @param createParents
   *          if true all parent dirs are created as well and no {@link ZkNodeExistsException} is
   *          thrown in case the
   *          path already exists
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistent(String path, boolean createParents)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createPersistent(path, createParents, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  /**
   * Create a persistent node with TTL and set its ACLs.
   * @param path the path where you want the node to be created
   * @param createParents if true all parent dirs are created as well and no
   *                      {@link ZkNodeExistsException} is thrown in case the path already exists
   * @param ttl TTL of the node in milliseconds
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistentWithTTL(String path, boolean createParents, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createPersistentWithTTL(path, createParents, ZooDefs.Ids.OPEN_ACL_UNSAFE, ttl);
  }

  /**
   * Create a container node and set its ACLs.
   * @param path the path where you want the node to be created
   * @param createParents if true all parent dirs are created as well and no
   *                      {@link ZkNodeExistsException} is thrown in case the path already exists
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createContainer(String path, boolean createParents)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createContainer(path, createParents, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  /**
   * Create a persistent node and set its ACLs.
   * @param path
   * @param acl
   *          List of ACL permissions to assign to the node
   * @param createParents
   *          if true all parent dirs are created as well and no {@link ZkNodeExistsException} is
   *          thrown in case the
   *          path already exists
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistent(String path, boolean createParents, List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    try {
      create(path, null, acl, CreateMode.PERSISTENT);
    } catch (ZkNodeExistsException e) {
      if (!createParents) {
        throw e;
      }
    } catch (ZkNoNodeException e) {
      if (!createParents) {
        throw e;
      }
      String parentDir = path.substring(0, path.lastIndexOf('/'));
      createPersistent(parentDir, createParents, acl);
      createPersistent(path, createParents, acl);
    }
  }

  /**
   * Create a persistent node with TTL and set its ACLs.
   * @param path the path where you want the node to be created
   * @param createParents if true all parent dirs are created as well and no
   *                      {@link ZkNodeExistsException} is thrown in case the path already exists
   * @param acl List of ACL permissions to assign to the node
   * @param ttl TTL of the node in milliseconds
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistentWithTTL(String path, boolean createParents, List<ACL> acl, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    try {
      create(path, null, acl, CreateMode.PERSISTENT_WITH_TTL, ttl);
    } catch (ZkNodeExistsException e) {
      if (!createParents) {
        throw e;
      }
    } catch (ZkNoNodeException e) {
      if (!createParents) {
        throw e;
      }
      String parentDir = path.substring(0, path.lastIndexOf('/'));
      createPersistentWithTTL(parentDir, createParents, acl, ttl);
      createPersistentWithTTL(path, createParents, acl, ttl);
    }
  }

  /**
   * Create a container node and set its ACLs.
   * @param path the path where you want the node to be created
   * @param createParents if true all parent dirs are created as well and no
   *                      {@link ZkNodeExistsException} is thrown in case the path already exists
   * @param acl List of ACL permissions to assign to the node
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createContainer(String path, boolean createParents, List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    try {
      create(path, null, acl, CreateMode.CONTAINER);
    } catch (ZkNodeExistsException e) {
      if (!createParents) {
        throw e;
      }
    } catch (ZkNoNodeException e) {
      if (!createParents) {
        throw e;
      }
      String parentDir = path.substring(0, path.lastIndexOf('/'));
      createContainer(parentDir, createParents, acl);
      createContainer(path, createParents, acl);
    }
  }

  /**
   * Create a persistent node.
   * @param path
   * @param data
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistent(String path, Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Create a persistent node with TTL.
   * @param path the path where you want the node to be created
   * @param data data of the node
   * @param ttl TTL of the node in milliseconds
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistentWithTTL(String path, Object data, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, CreateMode.PERSISTENT_WITH_TTL, ttl);
  }

  /**
   * Create a container node.
   * @param path the path where you want the node to be created
   * @param data data of the node
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createContainer(String path, Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, CreateMode.CONTAINER);
  }

  /**
   * Create a persistent node.
   * @param path
   * @param data
   * @param acl
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistent(String path, Object data, List<ACL> acl) {
    create(path, data, acl, CreateMode.PERSISTENT);
  }

  /**
   * Create a persistent node with TTL.
   * @param path the path where you want the node to be created
   * @param data data of the node
   * @param acl list of ACL for the node
   * @param ttl TTL of the node in milliseconds
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createPersistentWithTTL(String path, Object data, List<ACL> acl, long ttl) {
    create(path, data, acl, CreateMode.PERSISTENT_WITH_TTL, ttl);
  }

  /**
   * Create a container node.
   * @param path the path where you want the node to be created
   * @param data data of the node
   * @param acl list of ACL for the node
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createContainer(String path, Object data, List<ACL> acl) {
    create(path, data, acl, CreateMode.CONTAINER);
  }

  /**
   * Create a persistent, sequental node.
   * @param path
   * @param data
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createPersistentSequential(String path, Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  /**
   * Create a persistent, sequential node.
   * @param path the path where you want the node to be created
   * @param data data of the node
   * @param ttl TTL of the node in milliseconds
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createPersistentSequentialWithTTL(String path, Object data, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, ttl);
  }

  /**
   * Create a persistent, sequential node and set its ACL.
   * @param path
   * @param acl
   * @param data
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createPersistentSequential(String path, Object data, List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  /**
   * Create a persistent, sequential node and set its ACL.
   * @param path the path where you want the node to be created
   * @param acl list of ACL for the node
   * @param data data of the node
   * @param ttl TTL of the node in milliseconds
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createPersistentSequentialWithTTL(String path, Object data, List<ACL> acl, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, acl, CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, ttl);
  }

  /**
   * Create an ephemeral node.
   * @param path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, null, CreateMode.EPHEMERAL);
  }

  /**
   * Creates an ephemeral node. This ephemeral node is created by the expected(passed-in) ZK session.
   * If the expected session does not match the current ZK session, the node will not be created.
   *
   * @param path path of the node
   * @param sessionId expected session id of the ZK connection. If the session id of current ZK
   *                  connection does not match the expected session id, ephemeral creation will
   *                  fail
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path, final String sessionId)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createEphemeral(path, null, sessionId);
  }

  /**
   * Create an ephemeral node and set its ACL.
   * @param path
   * @param acl
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path, final List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, null, acl, CreateMode.EPHEMERAL);
  }

  /**
   * Creates an ephemeral node and set its ACL. This ephemeral node is created by the
   * expected(passed-in) ZK session. If the expected session does not match the current ZK session,
   * the node will not be created.
   *
   * @param path path of the ephemeral node
   * @param acl a list of ACL for the ephemeral node.
   * @param sessionId expected session id of the ZK connection. If the session id of current ZK
   *                  connection does not match the expected session id, ephemeral creation will
   *                  fail.
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path, final List<ACL> acl, final String sessionId)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, null, acl, CreateMode.EPHEMERAL, TTL_NOT_SET, sessionId);
  }

  /**
   * Create a node.
   * @param path
   * @param data
   * @param mode
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String create(final String path, Object data, final CreateMode mode)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
  }

  /**
   * Create a node.
   * @param path the path where you want the node to be created
   * @param data data of the node
   * @param mode {@link CreateMode} of the node
   * @param ttl TTL of the node in milliseconds, if mode is {@link CreateMode#PERSISTENT_WITH_TTL}
   *            or {@link CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String create(final String path, Object data, final CreateMode mode, long ttl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, ttl);
  }

  /**
   * Create a node with ACL.
   * @param path
   * @param datat
   * @param acl
   * @param mode
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String create(final String path, Object datat, final List<ACL> acl, final CreateMode mode)
      throws IllegalArgumentException, ZkException {
    return create(path, datat, acl, mode, TTL_NOT_SET, null);
  }

  /**
   * Create a node with ACL.
   * @param path the path where you want the node to be created
   * @param datat data of the node
   * @param acl list of ACL for the node
   * @param mode {@link CreateMode} of the node
   * @param ttl TTL of the node in milliseconds, if mode is {@link CreateMode#PERSISTENT_WITH_TTL}
   *            or {@link CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}
   * @return create node's path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String create(final String path, Object datat, final List<ACL> acl, final CreateMode mode,
      long ttl) throws IllegalArgumentException, ZkException {
    return create(path, datat, acl, mode, ttl, null);
  }

  /**
   * Creates a node and returns the actual path of the created node.
   *
   * Given an expected non-null session id, if the node is successfully created, it is guaranteed to
   * be created in the expected(passed-in) session.
   *
   * If the expected session is expired, which means the expected session does not match the current
   * session of ZK connection, the node will not be created.
   *
   * @param path the path where you want the node to be created
   * @param dataObject data of the node
   * @param acl list of ACL for the node
   * @param mode {@link CreateMode} of the node
   * @param ttl TTL of the node in milliseconds, if mode is {@link CreateMode#PERSISTENT_WITH_TTL}
   *            or {@link CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}
   * @param expectedSessionId the expected session ID of the ZK connection. It is not necessarily the
   *                  session ID of current ZK Connection. If the expected session ID is NOT null,
   *                  the node is guaranteed to be created in the expected session, or creation is
   *                  failed if the expected session id doesn't match current connected zk session.
   *                  If the session id is null, it means the create operation is NOT session aware.
   * @return path of the node created
   * @throws IllegalArgumentException if called from anything else except the ZooKeeper event thread
   * @throws ZkException if any zookeeper exception occurs
   */
  private String create(final String path, final Object dataObject, final List<ACL> acl,
      final CreateMode mode, long ttl, final String expectedSessionId)
      throws IllegalArgumentException, ZkException {
    if (path == null) {
      throw new NullPointerException("Path must not be null.");
    }
    if (acl == null || acl.size() == 0) {
      throw new NullPointerException("Missing value for ACL");
    }
    long startT = System.currentTimeMillis();
    try {
      final byte[] dataBytes = dataObject == null ? null : serialize(dataObject, path);
      checkDataSizeLimit(path, dataBytes);

      final String actualPath;
      if (mode.isTTL()) {
        actualPath = retryUntilConnected(() -> getExpectedZookeeper(expectedSessionId)
            .create(path, dataBytes, acl, mode, null, ttl));
      } else {
        actualPath = retryUntilConnected(() -> getExpectedZookeeper(expectedSessionId)
            .create(path, dataBytes, acl, mode));
      }

      record(path, dataBytes, startT, ZkClientMonitor.AccessType.WRITE);
      return actualPath;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.WRITE);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient {} create, path {}, time {} ms", _uid, path, (endT - startT));
      }
    }
  }

  /**
   * Create an ephemeral node.
   * @param path
   * @param data
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path, final Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, CreateMode.EPHEMERAL);
  }

  /**
   * Creates an ephemeral node. Given an expected non-null session id, if the ephemeral
   * node is successfully created, it is guaranteed to be in the expected(passed-in) session.
   *
   * If the expected session is expired, which means the expected session does not match the session
   * of current ZK connection, the ephemeral node will not be created.
   * If connection is timed out or interrupted, exception is thrown.
   *
   * @param path path of the ephemeral node being created
   * @param data data of the ephemeral node being created
   * @param sessionId the expected session ID of the ZK connection. It is not necessarily the
   *                  session ID of current ZK Connection. If the expected session ID is NOT null,
   *                  the node is guaranteed to be created in the expected session, or creation is
   *                  failed if the expected session id doesn't match current connected zk session.
   *                  If the session id is null, it means the operation is NOT session aware
   *                  and the node will be created by current ZK session.
   * @throws ZkInterruptedException if operation is interrupted, or a required reconnection gets
   *         interrupted
   * @throws IllegalArgumentException if called from anything except the ZooKeeper event thread
   * @throws ZkException if any ZooKeeper exception occurs
   * @throws RuntimeException if any other exception occurs
   */
  public void createEphemeral(final String path, final Object data, final String sessionId)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, TTL_NOT_SET, sessionId);
  }

  /**
   * Create an ephemeral node.
   * @param path
   * @param data
   * @param acl
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path, final Object data, final List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, acl, CreateMode.EPHEMERAL);
  }

  /**
   * Creates an ephemeral node in an expected ZK session. Given an expected non-null session id,
   * if the ephemeral node is successfully created, it is guaranteed to be in the expected session.
   * If the expected session is expired, which means the expected session does not match the session
   * of current ZK connection, the ephemeral node will not be created.
   * If connection is timed out or interrupted, exception is thrown.
   *
   * @param path path of the ephemeral node being created
   * @param data data of the ephemeral node being created
   * @param acl list of ACL for the ephemeral node
   * @param sessionId the expected session ID of the ZK connection. It is not necessarily the
   *                  session ID of current ZK Connection. If the expected session ID is NOT null,
   *                  the node is guaranteed to be created in the expected session, or creation is
   *                  failed if the expected session id doesn't match current connected zk session.
   *                  If the session id is null, it means the create operation is NOT session aware
   *                  and the node will be created by current ZK session.
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public void createEphemeral(final String path, final Object data, final List<ACL> acl,
      final String sessionId)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, acl, CreateMode.EPHEMERAL, TTL_NOT_SET, sessionId);
  }

  /**
   * Create an ephemeral, sequential node.
   * @param path
   * @param data
   * @return created path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createEphemeralSequential(final String path, final Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  /**
   * Creates an ephemeral, sequential node with ACL in an expected ZK session.
   * Given an expected non-null session id, if the ephemeral node is successfully created,
   * it is guaranteed to be in the expected session.
   * If the expected session is expired, which means the expected session does not match the session
   * of current ZK connection, the ephemeral node will not be created.
   * If connection is timed out or interrupted, exception is thrown.
   *
   * @param path path of the node
   * @param data data of the node
   * @param acl list of ACL for the node
   * @return created path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createEphemeralSequential(final String path, final Object data, final List<ACL> acl,
      final String sessionId)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, acl, CreateMode.EPHEMERAL_SEQUENTIAL, TTL_NOT_SET, sessionId);
  }

  /**
   * Creates an ephemeral, sequential node. Given an expected non-null session id,
   * if the ephemeral node is successfully created, it is guaranteed to be in the expected session.
   * If the expected session is expired, which means the expected session does not match the session
   * of current ZK connection, the ephemeral node will not be created.
   * If connection is timed out or interrupted, exception is thrown.
   *
   * @param path path of the node
   * @param data data of the node
   * @param sessionId the expected session ID of the ZK connection. It is not necessarily the
   *                  session ID of current ZK Connection. If the expected session ID is NOT null,
   *                  the node is guaranteed to be created in the expected session, or creation is
   *                  failed if the expected session id doesn't match current connected zk session.
   *                  If the session id is null, it means the create operation is NOT session aware
   *                  and the node will be created by current ZK session.
   * @return created path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createEphemeralSequential(final String path, final Object data,
      final String sessionId)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
        TTL_NOT_SET, sessionId);
  }

  /**
   * Create an ephemeral, sequential node with ACL.
   * @param path
   * @param data
   * @param acl
   * @return created path
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs
   */
  public String createEphemeralSequential(final String path, final Object data, final List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, acl, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  @Override
  public void process(WatchedEvent event) {
    long notificationTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("zkclient {}, Received event: {} ", _uid, event);
    }
    _zookeeperEventThread = Thread.currentThread();

    boolean stateChanged = event.getPath() == null;
    boolean sessionExpired = stateChanged && event.getState() == KeeperState.Expired;
    boolean znodeChanged = event.getPath() != null;
    boolean dataChanged =
        event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted
            || event.getType() == EventType.NodeCreated
            || event.getType() == EventType.NodeChildrenChanged;
    if (event.getType() == EventType.NodeDeleted) {
      LOG.debug("zkclient {}, Path {} is deleted", _uid, event.getPath());
    }

    getEventLock().lock();
    try {
      // We might have to install child change event listener if a new node was created
      if (getShutdownTrigger()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("zkclient {} ignoring event {}|{} since shutdown triggered",
              _uid, event.getType(), event.getPath());
        }
        return;
      }
      if (stateChanged) {
        processStateChanged(event);
      }
      if (dataChanged) {
        processDataOrChildChange(event, notificationTime);
      }
    } finally {
      if (stateChanged) {
        getEventLock().getStateChangedCondition().signalAll();

        // If the session expired we have to signal all conditions, because watches might have been
        // removed and
        // there is no guarantee that those
        // conditions will be signaled at all after an Expired event
        // TODO PVo write a test for this
        if (event.getState() == KeeperState.Expired) {
          getEventLock().getZNodeEventCondition().signalAll();
          getEventLock().getDataChangedCondition().signalAll();
        }
      }
      if (znodeChanged) {
        getEventLock().getZNodeEventCondition().signalAll();
      }
      if (dataChanged) {
        getEventLock().getDataChangedCondition().signalAll();
      }
      getEventLock().unlock();

      // update state change counter.
      recordStateChange(stateChanged, dataChanged, sessionExpired);

      if (LOG.isDebugEnabled()) {
        LOG.debug("zkclient {} Leaving process event", _uid);
      }
    }
  }

  private void fireAllEvents() {
    //TODO: During handling new session, if the path is deleted, watcher leakage could still happen
    for (Entry<String, Set<IZkChildListener>> entry : _childListener.entrySet()) {
      fireChildChangedEvents(entry.getKey(), entry.getValue(), true);
    }
    for (Entry<String, Set<IZkDataListenerEntry>> entry : _dataListener.entrySet()) {
      fireDataChangedEvents(entry.getKey(), entry.getValue(), OptionalLong.empty(), true);
    }
  }

  /**
   * Returns a list of children of the given path.
   * <p>
   * NOTE: if the given path has too many children which causes the network packet length to exceed
   * {@code jute.maxbuffer}, there are 2 cases, depending on whether or not the native
   * zk supports paginated getChildren API and the config
   * {@link ZkSystemPropertyKeys#ZK_GETCHILDREN_PAGINATION_DISABLED}:
   * <p>1) pagination is disabled by {@link ZkSystemPropertyKeys#ZK_GETCHILDREN_PAGINATION_DISABLED}
   * set to true or zk does not support pagination: the operation will fail.
   * <p>2) config is false and zk supports pagination. A list of all children will be fetched using
   * pagination and returned. But please note that the final children list is NOT strongly
   * consistent with server - the list might contain some deleted children if some children
   * are deleted before the last page is fetched. The upstream caller should be able to handle this.
   */
  public List<String> getChildren(String path) {
    return getChildren(path, hasListeners(path));
  }

  protected List<String> getChildren(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();

    try {
      List<String> children = retryUntilConnected(new Callable<List<String>>() {
        private int connectionLossRetryCount = 0;

        @Override
        public List<String> call() throws Exception {
          try {
            return getConnection().getChildren(path, watch);
          } catch (ConnectionLossException e) {
            // Issue: https://github.com/apache/helix/issues/962
            // Connection loss might be caused by an excessive number of children.
            // Infinitely retrying connecting may cause high GC in ZK server and kill ZK server.
            // This is a workaround to check numChildren to have a chance to exit retry loop.
            // Check numChildren stat every other 3 connection loss, because there is a higher
            // possibility that connection loss is caused by other factors such as network
            // connectivity, session expired, etc.
            // TODO: remove this check once we have a better way to exit infinite retry
            ++connectionLossRetryCount;
            if (connectionLossRetryCount >= 3) {
              checkNumChildrenLimit(path);
              connectionLossRetryCount = 0;
            }

            // Re-throw the ConnectionLossException for retryUntilConnected() to catch and retry.
            throw e;
          }
        }
      });
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      return children;
    } catch (ZkNoNodeException e) {
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      throw e;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient {} getChildren, path {} time: {} ms", _uid, path, (endT - startT) );
      }
    }
  }

  /**
   * Counts number of children for the given path.
   * @param path
   * @return number of children or 0 if path does not exist.
   */
  public int countChildren(String path) {
    try {
      return getChildren(path).size();
    } catch (ZkNoNodeException e) {
      return 0;
    }
  }

  public boolean exists(final String path) {
    return exists(path, hasListeners(path));
  }

  protected boolean exists(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();
    try {
      boolean exists = retryUntilConnected(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return getConnection().exists(path, watch);
        }
      });
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      return exists;
    } catch (ZkNoNodeException e) {
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      throw e;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient exists, path: {}, time: {} ms", _uid, path, (endT - startT));
      }
    }
  }

  public Stat getStat(final String path) {
    return getStat(path, false);
  }

  private Stat getStat(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();
    final Stat stat;
    try {
      stat = retryUntilConnected(
          () -> ((ZkConnection) getConnection()).getZookeeper().exists(path, watch));
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      return stat;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient exists, path: {}, time: {} ms", _uid, path, (endT - startT));
      }
    }
  }

  /*
   * This one installs watch only if path is there. Meant to avoid leaking watch in Zk server.
   */
  private Stat installWatchOnlyPathExist(final String path) {
    long startT = System.currentTimeMillis();
    final Stat stat;
    try {
        stat = new Stat();
        try {
          LOG.debug("installWatchOnlyPathExist with path: {} ", path);
          retryUntilConnected(() -> ((ZkConnection) getConnection()).getZookeeper().getData(path, true, stat));
        } catch (ZkNoNodeException e) {
          LOG.debug("installWatchOnlyPathExist path not existing: {}", path);
          record(path, null, startT, ZkClientMonitor.AccessType.READ);
          return null;
        }
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      return stat;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient getData (installWatchOnlyPathExist), path: {}, time: {} ms",
            _uid, path, (endT - startT));
      }
    }
  }

  protected void processStateChanged(WatchedEvent event) {
    LOG.info("zkclient {}, zookeeper state changed ( {} )", _uid, event.getState());
    setCurrentState(event.getState());
    if (getShutdownTrigger()) {
      return;
    }

    fireStateChangedEvent(event.getState());

    /*
     *  Note, the intention is that only the ZkClient managing the session would do auto reconnect
     *  and fireNewSessionEvents and fireAllEvent.
     *  Other ZkClient not managing the session would only fireAllEvent upon a new session.
     */
    if (event.getState() == KeeperState.SyncConnected) {
      if (!_isNewSessionEventFired && !"0".equals(getHexSessionId())) {
        /*
         * Before the new zookeeper instance is connected to the zookeeper service and its session
         * is established, its session id is 0.
         * New session event is not fired until the new zookeeper session receives the first
         * SyncConnected state(the zookeeper session is established).
         * Now the session id is available and non-zero, and we can fire new session events.
         */
        fireNewSessionEvents();

        /*
         * Set it true to avoid firing events again for the same session next time
         * when SyncConnected events are received.
         */
        _isNewSessionEventFired = true;

        /*
         * With this first SyncConnected state, we just get connected to zookeeper service after
         * reconnecting when the session expired. Because previous session expired, we also have to
         * notify all listeners that something might have changed.
         */
        fireAllEvents();
      }
    } else if (event.getState() == KeeperState.Expired) {
      _isNewSessionEventFired = false;
      reconnectOnExpiring();
    }
  }

  private void reconnectOnExpiring() {
    // only managing zkclient reconnect
    if (!isManagingZkConnection()) {
      return;
    }
    int retryCount = 0;
    ExponentialBackoffStrategy retryStrategy =
        new ExponentialBackoffStrategy(MAX_RECONNECT_INTERVAL_MS, true);

    Exception reconnectException = new ZkException("Shutdown triggered.");
    while (!isClosed()) {
      try {
        reconnect();
        return;
      } catch (ZkInterruptedException interrupt) {
        reconnectException = interrupt;
        break;
      } catch (Exception e) {
        reconnectException = e;
        long waitInterval = retryStrategy.getNextWaitInterval(retryCount++);
        LOG.warn("ZkClient {}, reconnect on expiring failed. Will retry after {} ms",
            _uid, waitInterval, e);
        try {
          Thread.sleep(waitInterval);
        } catch (InterruptedException ex) {
          reconnectException = ex;
          break;
        }
      }
    }

    LOG.info("Zkclient {} unable to re-establish connection. Notifying consumer of the following exception:{}",
        _uid, reconnectException);
    fireSessionEstablishmentError(reconnectException);
  }

  private void reconnect() {
    getEventLock().lock();
    try {
      ZkConnection connection = ((ZkConnection) getConnection());
      connection.reconnect(this);
    } catch (InterruptedException e) {
      throw new ZkInterruptedException(e);
    } finally {
      getEventLock().unlock();
    }
  }

  private void doAsyncSync(final ZooKeeper zk, final String path, final long startT,
      final ZkAsyncCallbacks.SyncCallbackHandler cb) {
    try {
      zk.sync(path, cb,
          new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, _monitor, startT, 0, true) {
            @Override
            protected void doRetry() throws Exception {
              doAsyncSync(zk, path, System.currentTimeMillis(), cb);
            }
          });
    } catch (RuntimeException e) {
      // Process callback to release caller from waiting
      cb.processResult(KeeperException.Code.APIERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, true));
      throw e;
    }
  }


  /*
   *  Note, issueSync takes a ZooKeeper (client) object and pass it to doAsyncSync().
   *  The reason we do this is that we want to ensure each new session event is preceded with exactly
   *  one sync() to server. The sync() is to make sure the server would not see stale data.
   *
   *  ZooKeeper client object has an invariant of each object has one session. With this invariant
   *  we can achieve each one sync() to server upon new session establishment. The reasoning is:
   *  issueSync() is called when fireNewSessionEvents() which in under eventLock of ZkClient. Thus
   *  we are guaranteed the ZooKeeper object passed in would have the new incoming sessionId. If by
   *  the time sync() is invoked, the session expires. The sync() would fail with a stale session.
   *  This is exactly what we want. The newer session would ensure another fireNewSessionEvents.
   */
  private boolean issueSync(ZooKeeper zk) {
    String sessionId = Long.toHexString(zk.getSessionId());
    ZkAsyncCallbacks.SyncCallbackHandler callbackHandler =
        new ZkAsyncCallbacks.SyncCallbackHandler(sessionId);

    final long startT = System.currentTimeMillis();
    doAsyncSync(zk, SYNC_PATH, startT, callbackHandler);

    callbackHandler.waitForSuccess();

    KeeperException.Code code = KeeperException.Code.get(callbackHandler.getRc());
    if (code == KeeperException.Code.OK) {
      LOG.info("zkclient {}, sycnOnNewSession with sessionID {} async return code: {} and proceeds",
          _uid, sessionId, code);
      return true;
    }

    // Not retryable error, including session expiration; return false.
    return false;
  }

  private void fireNewSessionEvents() {
    // only managing zkclient fire handleNewSession event
    if (!isManagingZkConnection()) {
      return;
    }
    final String sessionId = getHexSessionId();

    if (SYNC_ON_SESSION) {
      final ZooKeeper zk = ((ZkConnection) getConnection()).getZookeeper();
      _eventThread.send(new ZkEventThread.ZkEvent("Sync call before new session event of session " + sessionId,
          sessionId) {
        @Override
        public void run() throws Exception {
          if (issueSync(zk) == false) {
            LOG.warn("zkclient{}, Failed to call sync() on new session {}", _uid, sessionId);
          }
        }
      });
    }

    for (final IZkStateListener stateListener : _stateListener) {
      _eventThread
          .send(new ZkEventThread.ZkEvent("New session event sent to " + stateListener, sessionId) {

            @Override
            public void run() throws Exception {
              stateListener.handleNewSession(sessionId);
            }
          });
    }
  }

  protected void fireStateChangedEvent(final KeeperState state) {
    final String sessionId = getHexSessionId();
    for (final IZkStateListener stateListener : _stateListener) {
      final String description = "State changed to " + state + " sent to " + stateListener;
      _eventThread.send(new ZkEventThread.ZkEvent(description, sessionId) {

        @Override
        public void run() throws Exception {
          stateListener.handleStateChanged(state);
        }
      });
    }
  }

  private void fireSessionEstablishmentError(final Throwable error) {
    for (final IZkStateListener stateListener : _stateListener) {
      _eventThread
          .send(new ZkEventThread.ZkEvent("Session establishment error(" + error + ") sent to " + stateListener) {

            @Override
            public void run() throws Exception {
              stateListener.handleSessionEstablishmentError(error);
            }
          });
    }
  }

  private boolean hasListeners(String path) {
    Set<IZkDataListenerEntry> dataListeners = _dataListener.get(path);
    if (dataListeners != null && dataListeners.size() > 0) {
      return true;
    }
    Set<IZkChildListener> childListeners = _childListener.get(path);
    if (childListeners != null && childListeners.size() > 0) {
      return true;
    }
    return false;
  }

  /**
   * Delete the path as well as all its children.
   * This method is deprecated, please use {@link #deleteRecursively(String)}} instead
   * @param path ZK path
   * @return true if successfully deleted all children, and the given path, else false
   */
  @Deprecated
  public boolean deleteRecursive(String path) {
    try {
      deleteRecursively(path);
      return true;
    } catch (ZkClientException e) {
      LOG.error("zkcient {}, Failed to recursively delete path {}, exception {}",
          _uid, path, e);
      return false;
    }
  }

  /**
   * Delete the path as well as all its children.
   * @param path
   * @throws ZkClientException
   */
  public void deleteRecursively(String path) throws ZkClientException {
    List<String> children;
    try {
      children = getChildren(path, false);
    } catch (ZkNoNodeException e) {
      // if the node to be deleted does not exist, treat it as success.
      return;
    }

    for (String subPath : children) {
      deleteRecursively(path + "/" + subPath);
    }

    // delete() function call will return true if successful, false if the path does not
    // exist (in this context, it should be treated as successful), and throw exception
    // if there is any other failure case.
    try {
      delete(path);
    } catch (Exception e) {
      LOG.error("zkclient {}, Failed to delete {}, exception {}", _uid, path, e);
      throw new ZkClientException("Failed to delete " + path, e);
    }
  }

  private void processDataOrChildChange(WatchedEvent event, long notificationTime) {
    final String path = event.getPath();
    final boolean pathExists = event.getType() != EventType.NodeDeleted;
    if (EventType.NodeDeleted == event.getType()) {
      LOG.debug("zkclient{}, Event NodeDeleted: {}", _uid, event.getPath());
    }

    if (event.getType() == EventType.NodeChildrenChanged || event.getType() == EventType.NodeCreated
        || event.getType() == EventType.NodeDeleted) {
      Set<IZkChildListener> childListeners = _childListener.get(path);
      if (childListeners != null && !childListeners.isEmpty()) {
        // TODO recording child changed event propagation latency as well. Note this change will
        // introduce additional ZK access.
        fireChildChangedEvents(path, childListeners, pathExists);
      }
    }

    if (event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted
        || event.getType() == EventType.NodeCreated) {
      Set<IZkDataListenerEntry> listeners = _dataListener.get(path);
      if (listeners != null && !listeners.isEmpty()) {
        fireDataChangedEvents(event.getPath(), listeners, OptionalLong.of(notificationTime),
            pathExists);
      }
    }
  }

  private void fireDataChangedEvents(final String path, Set<IZkDataListenerEntry> listeners,
      final OptionalLong notificationTime, boolean pathExists) {
    try {
      final ZkPathStatRecord pathStatRecord = new ZkPathStatRecord(path);
      // Trigger listener callbacks
      for (final IZkDataListenerEntry listener : listeners) {
        _eventThread.send(new ZkEventThread.ZkEvent(
            "Data of " + path + " changed sent to " + listener.getDataListener()
                + " prefetch data: " + listener.isPrefetchData()) {
          @Override
          public void run() throws Exception {
            if (!pathStatRecord.pathChecked()) {
              // getStat() wrapp two ways to install data watch by using exists() or getData().
              // getData() aka useGetData (true) would not install the watch if the node not ]
              // existing. Exists() aka useGetData (false) would install (leak) the watch if the
              // node not existing.
              // Here the goal is to avoid leaking watch. Thus, if we know path not exists, we use
              // the exists() useGetData (false) route to check stat. Otherwise, we use getData()
              // to install watch.
              Stat stat = null;
              if (!pathExists) {
                stat = getStat(path, false);
              } else {
                stat = installWatchOnlyPathExist(path);
              }
              pathStatRecord.recordPathStat(stat, notificationTime);
            }
            if (!pathStatRecord.pathExists()) {
              listener.getDataListener().handleDataDeleted(path);
            } else {
              Object data = null;
              if (listener.isPrefetchData()) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("zkclient {} Prefetch data for path: {}", _uid, path);
                }
                try {
                  // TODO: the data is redundantly read multiple times when multiple listeners exist
                  data = readData(path, null, true);
                } catch (ZkNoNodeException e) {
                  LOG.warn("zkclient {} Prefetch data for path: {} failed.", _uid, path, e);
                  listener.getDataListener().handleDataDeleted(path);
                  return;
                }
              }
              listener.getDataListener().handleDataChange(path, data);
            }
          }
        });
      }
    } catch (Exception e) {
      LOG.error("zkclient {} Failed to fire data changed event for path: {}", _uid, path, e);
    }
  }

  private void fireChildChangedEvents(final String path, Set<IZkChildListener> childListeners, boolean pathExists) {
    try {
      final ZkPathStatRecord pathStatRecord = new ZkPathStatRecord(path);
      for (final IZkChildListener listener : childListeners) {
        _eventThread.send(new ZkEventThread.ZkEvent("Children of " + path + " changed sent to " + listener) {
          @Override
          public void run() throws Exception {
            if (!pathStatRecord.pathChecked()) {
              Stat stat = null;
              if (!pathExists || !hasListeners(path)) {
                // will not install listener using exists call
                stat = getStat(path, false);
              } else {
                // will install listener using getData() call; if node not there, install nothing
                stat = installWatchOnlyPathExist(path);
              }
              pathStatRecord.recordPathStat(stat, OptionalLong.empty());
            }
            List<String> children = null;
            if (pathStatRecord.pathExists()) {
              try {
                children = getChildren(path);
              } catch (ZkNoNodeException e) {
                LOG.warn("zkclient {} Get children under path: {} failed.", _uid, path, e);
                // Continue trigger the change handler
              }
            }
            listener.handleChildChange(path, children);
          }
        });
      }
    } catch (Exception e) {
      LOG.error("zkclient {} Failed to fire child changed event. Unable to getChildren.", _uid, e);
    }
  }

  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time)
      throws ZkInterruptedException {
    Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Waiting until znode {} becomes available.", _uid, path);
    }
    if (exists(path)) {
      return true;
    }
    acquireEventLock();
    try {
      while (!exists(path, true)) {
        boolean gotSignal = getEventLock().getZNodeEventCondition().awaitUntil(timeout);
        if (!gotSignal) {
          return false;
        }
      }
      return true;
    } catch (InterruptedException e) {
      throw new ZkInterruptedException(e);
    } finally {
      getEventLock().unlock();
    }
  }

  public IZkConnection getConnection() {
    return _connection;
  }

  public long waitForEstablishedSession(long timeout, TimeUnit timeUnit) {
    validateCurrentThread();

    acquireEventLock();
    try {
      if (!waitForKeeperState(KeeperState.SyncConnected, timeout, timeUnit)) {
        throw new ZkTimeoutException("Waiting to be connected to ZK server has timed out.");
      }
      // Reading session ID before unlocking event lock is critical to guarantee the established
      // session's ID won't change.
      return getSessionId();
    } finally {
      getEventLock().unlock();
    }
  }

  public boolean waitUntilConnected(long time, TimeUnit timeUnit) throws ZkInterruptedException {
    return waitForKeeperState(KeeperState.SyncConnected, time, timeUnit);
  }

  public boolean waitForKeeperState(KeeperState keeperState, long time, TimeUnit timeUnit)
      throws ZkInterruptedException {
    validateCurrentThread();
    Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

    LOG.debug("zkclient {}, Waiting for keeper state {} ", _uid, keeperState);
    acquireEventLock();
    try {
      boolean stillWaiting = true;
      while (_currentState != keeperState) {
        if (!stillWaiting) {
          return false;
        }
        stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeout);
      }
      LOG.debug("zkclient {} State is {}",
          _uid, (_currentState == null ? "CLOSED" : _currentState));
      return true;
    } catch (InterruptedException e) {
      throw new ZkInterruptedException(e);
    } finally {
      getEventLock().unlock();
    }
  }

  private void acquireEventLock() {
    try {
      getEventLock().lockInterruptibly();
    } catch (InterruptedException e) {
      throw new ZkInterruptedException(e);
    }
  }

  /**
   * @param <T>
   * @param callable
   * @return result of Callable
   * @throws ZkInterruptedException
   *           if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *           if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *           if any ZooKeeper exception occurred
   * @throws RuntimeException
   *           if any other exception occurs from invoking the Callable
   */
  public <T> T retryUntilConnected(final Callable<T> callable)
      throws IllegalArgumentException, ZkException {
    if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
      throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
    }
    final long operationStartTime = System.currentTimeMillis();
    if (_monitor != null) {
      _monitor.increaseOutstandingRequestGauge();
    }
    try {
      while (true) {
        // Because ConnectionLossException and SessionExpiredException are caught but not thrown,
        // we don't know what causes retry. This is used to record which one of the two exceptions
        // causes retry in ZkTimeoutException.
        // This also helps the test testConnectionLossWhileCreateEphemeral.
        KeeperException.Code retryCauseCode;

        if (isClosed()) {
          throw new IllegalStateException("ZkClient already closed!");
        }
        try {
          final ZkConnection zkConnection = (ZkConnection) getConnection();
          // Validate that the connection is not null before trigger callback
          if (zkConnection == null || zkConnection.getZookeeper() == null) {
            throw new IllegalStateException(
                "ZkConnection is in invalid state! Please close this ZkClient and create new client.");
          }
          return callable.call();
        } catch (ConnectionLossException e) {
          retryCauseCode = e.code();
          // we give the event thread some time to update the status to 'Disconnected'
          Thread.yield();
          waitForRetry();
        } catch (SessionExpiredException e) {
          retryCauseCode = e.code();
          // we give the event thread some time to update the status to 'Expired'
          Thread.yield();
          waitForRetry();
        } catch (ZkSessionMismatchedException e) {
          throw e;
        } catch (KeeperException e) {
          throw ZkException.create(e);
        } catch (InterruptedException e) {
          throw new ZkInterruptedException(e);
        } catch (Exception e) {
          throw ExceptionUtil.convertToRuntimeException(e);
        }

        LOG.debug("zkclient {}, Retrying operation, caused by {}", _uid,retryCauseCode);
        // before attempting a retry, check whether retry timeout has elapsed
        if (System.currentTimeMillis() - operationStartTime > _operationRetryTimeoutInMillis) {
          throw new ZkTimeoutException("Operation cannot be retried because of retry timeout ("
              + _operationRetryTimeoutInMillis + " milli seconds). Retry was caused by "
              + retryCauseCode);
        }
      }
    } finally {
      if (_monitor != null) {
        _monitor.decreaseOutstandingRequestGauge();
      }
    }
  }

  private void waitForRetry() {
    waitUntilConnected(_operationRetryTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  public void setCurrentState(KeeperState currentState) {
    getEventLock().lock();
    try {
      _currentState = currentState;
    } finally {
      getEventLock().unlock();
    }
  }

  /**
   * Returns a mutex all zookeeper events are synchronized aginst. So in case you need to do
   * something without getting
   * any zookeeper event interruption synchronize against this mutex. Also all threads waiting on
   * this mutex object
   * will be notified on an event.
   * @return the mutex.
   */
  public ZkLock getEventLock() {
    return _zkEventLock;
  }

  /**
   * Delete the given path. Path should not have any children or the deletion will fail.
   * This function will throw exception if we fail to delete an existing path
   * @param path
   * @return true if path is successfully deleted, false if path does not exist
   */
  public boolean delete(final String path) {
    long startT = System.currentTimeMillis();
    boolean success;
    try {
      try {
        retryUntilConnected(new Callable<Object>() {

          @Override
          public Object call() throws Exception {
            getConnection().delete(path);
            return null;
          }
        });
        success = true;
      } catch (ZkNoNodeException e) {
        success = false;
        LOG.debug("zkclient {}, Failed to delete path {}, znode does not exist!", _uid, path, e);
      }
      record(path, null, startT, ZkClientMonitor.AccessType.WRITE);
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.WRITE);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient {} delete, path: {}, time {} ms", _uid, path, (endT - startT));
      }
    }
    return success;
  }

  public void setZkSerializer(ZkSerializer zkSerializer) {
    _pathBasedZkSerializer = new BasicZkSerializer(zkSerializer);
  }

  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
    _pathBasedZkSerializer = zkSerializer;
  }

  public PathBasedZkSerializer getZkSerializer() {
    return _pathBasedZkSerializer;
  }

  public byte[] serialize(Object data, String path) {
    return _pathBasedZkSerializer.serialize(data, path);
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T deserialize(byte[] data, String path) {
    if (data == null) {
      return null;
    }
    return (T) _pathBasedZkSerializer.deserialize(data, path);
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readData(String path) {
    return (T) readData(path, false);
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readData(String path, boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      data = (T) readData(path, null);
    } catch (ZkNoNodeException e) {
      if (!returnNullIfPathNotExists) {
        throw e;
      }
    }
    return data;
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readData(String path, Stat stat) {
    return (T) readData(path, stat, hasListeners(path));
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readData(final String path, final Stat stat, final boolean watch) {
    long startT = System.currentTimeMillis();
    byte[] data = null;
    try {
      data = retryUntilConnected(new Callable<byte[]>() {

        @Override
        public byte[] call() throws Exception {
          return getConnection().readData(path, stat, watch);
        }
      });
      record(path, data, startT, ZkClientMonitor.AccessType.READ);
      return (T) deserialize(data, path);
    } catch (ZkNoNodeException e) {
      record(path, data, startT, ZkClientMonitor.AccessType.READ);
      throw e;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient {}, getData, path {}, time {} ms", _uid, path, (endT - startT));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path, Stat stat,
      boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      data = readData(path, stat);
    } catch (ZkNoNodeException e) {
      if (!returnNullIfPathNotExists) {
        throw e;
      }
    }
    return data;
  }

  public void writeData(String path, Object object) {
    writeData(path, object, -1);
  }

  /**
   * Updates data of an existing znode. The current content of the znode is passed to the
   * {@link DataUpdater} that is
   * passed into this method, which returns the new content. The new content is only written back to
   * ZooKeeper if
   * nobody has modified the given znode in between. If a concurrent change has been detected the
   * new data of the
   * znode is passed to the updater once again until the new contents can be successfully written
   * back to ZooKeeper.
   * @param <T>
   * @param path
   *          The path of the znode.
   * @param updater
   *          Updater that creates the new contents.
   */
  @SuppressWarnings("unchecked")
  public <T extends Object> void updateDataSerialized(String path, DataUpdater<T> updater) {
    Stat stat = new Stat();
    boolean retry;
    do {
      retry = false;
      try {
        T oldData = (T) readData(path, stat);
        T newData = updater.update(oldData);
        writeData(path, newData, stat.getVersion());
      } catch (ZkBadVersionException e) {
        retry = true;
      }
    } while (retry);
  }

  public void writeData(final String path, Object datat, final int expectedVersion) {
    writeDataReturnStat(path, datat, expectedVersion);
  }

  public Stat writeDataReturnStat(final String path, Object datat, final int expectedVersion) {
    long startT = System.currentTimeMillis();
    try {
      final byte[] data = serialize(datat, path);
      checkDataSizeLimit(path, data);
      final Stat stat = (Stat) retryUntilConnected(
          (Callable<Object>) () -> getConnection().writeDataReturnStat(path, data, expectedVersion));
      record(path, data, startT, ZkClientMonitor.AccessType.WRITE);
      return stat;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.WRITE);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("zkclient {}, setData, path {}, time {} ms", _uid, path, (endT - startT));
      }
    }
  }

  public Stat writeDataGetStat(final String path, Object datat, final int expectedVersion) {
    return writeDataReturnStat(path, datat, expectedVersion);
  }

  public void asyncCreate(final String path, Object datat, final CreateMode mode,
      final ZkAsyncCallbacks.CreateCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data;
    try {
      data = (datat == null ? null : serialize(datat, path));
    } catch (ZkMarshallingError e) {
      cb.processResult(KeeperException.Code.MARSHALLINGERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, false), null);
      return;
    }
    doAsyncCreate(path, data, mode, TTL_NOT_SET, startT, cb, parseExpectedSessionId(datat));
  }

  private void doAsyncCreate(final String path, final byte[] data, final CreateMode mode, long ttl,
      final long startT, final ZkAsyncCallbacks.CreateCallbackHandler cb, final String expectedSessionId) {
    try {
      retryUntilConnected(() -> {
        getExpectedZookeeper(expectedSessionId).create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, cb,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, _monitor, startT, 0, false,
                GZipCompressionUtil.isCompressed(data)) {
              @Override
              protected void doRetry() {
                doAsyncCreate(path, data, mode, ttl, System.currentTimeMillis(), cb, expectedSessionId);
              }
            }, ttl);
        return null;
      });
    } catch (RuntimeException e) {
      // Process callback to release caller from waiting
      cb.processResult(KeeperException.Code.APIERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, false), null, null);
      throw e;
    }
  }

  public void asyncCreate(final String path, Object datat, final CreateMode mode, long ttl,
      final ZkAsyncCallbacks.CreateCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data;
    try {
      data = (datat == null ? null : serialize(datat, path));
    } catch (ZkMarshallingError e) {
      cb.processResult(KeeperException.Code.MARSHALLINGERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, false), null, null);
      return;
    }
    doAsyncCreate(path, data, mode, ttl, startT, cb, parseExpectedSessionId(datat));
  }

  // Async Data Accessors
  public void asyncSetData(final String path, Object datat, final int version,
      final ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data;
    try {
      data = serialize(datat, path);
    } catch (ZkMarshallingError e) {
      cb.processResult(KeeperException.Code.MARSHALLINGERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, false), null);
      return;
    }
    doAsyncSetData(path, data, version, startT, cb, parseExpectedSessionId(datat));
  }

  private void doAsyncSetData(final String path, byte[] data, final int version, final long startT,
      final ZkAsyncCallbacks.SetDataCallbackHandler cb, final String expectedSessionId) {
    try {
      retryUntilConnected(() -> {
        getExpectedZookeeper(expectedSessionId).setData(path, data, version, cb,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, _monitor, startT, data == null ? 0 : data.length,
                false, GZipCompressionUtil.isCompressed(data)) {
              @Override
              protected void doRetry() {
                doAsyncSetData(path, data, version, System.currentTimeMillis(), cb, expectedSessionId);
              }
            });
        return null;
      });
    } catch (RuntimeException e) {
      // Process callback to release caller from waiting
      cb.processResult(KeeperException.Code.APIERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, false), null);
      throw e;
    }
  }

  public void asyncGetData(final String path, final ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    try {
      retryUntilConnected(() -> {
        ((ZkConnection) getConnection()).getZookeeper().getData(path, null, cb,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, _monitor, startT, 0, true) {
              @Override
              protected void doRetry() {
                asyncGetData(path, cb);
              }
            });
        return null;
      });
    } catch (RuntimeException e) {
      // Process callback to release caller from waiting
      cb.processResult(KeeperException.Code.APIERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, true), null, null);
      throw e;
    }
  }

  public void asyncExists(final String path, final ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    try {
      retryUntilConnected(() -> {
        ((ZkConnection) getConnection()).getZookeeper().exists(path, null, cb,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, _monitor, startT, 0, true) {
              @Override
              protected void doRetry() {
                asyncExists(path, cb);
              }
            });
        return null;
      });
    } catch (RuntimeException e) {
      // Process callback to release caller from waiting
      cb.processResult(KeeperException.Code.APIERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, true), null);
      throw e;
    }
  }

  public void asyncDelete(final String path, final ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    try {
      retryUntilConnected(() -> {
        ((ZkConnection) getConnection()).getZookeeper().delete(path, -1, cb,
            new ZkAsyncRetryCallContext(_asyncCallRetryThread, cb, _monitor, startT, 0, false) {
              @Override
              protected void doRetry() {
                asyncDelete(path, cb);
              }
            });
        return null;
      });
    } catch (RuntimeException e) {
      // Process callback to release caller from waiting
      cb.processResult(KeeperException.Code.APIERROR.intValue(), path,
          new ZkAsyncCallMonitorContext(_monitor, startT, 0, false));
      throw e;
    }
  }

  private void checkDataSizeLimit(String path, byte[] data) {
    if (data == null) {
      return;
    }

    if (data.length > WRITE_SIZE_LIMIT) {
      throw new ZkClientException("Data size of path " + path
          + " is greater than write size limit "
          + WRITE_SIZE_LIMIT + " bytes");
    }
  }

  public void watchForData(final String path) {
    watchForData(path, false);
  }

  private boolean watchForData(final String path, boolean skipWatchingNonExistNode) {
    try {
      if (skipWatchingNonExistNode) {
        retryUntilConnected(() -> (((ZkConnection) getConnection()).getZookeeper().getData(path, true, new Stat())));
      } else {
        retryUntilConnected(() -> (((ZkConnection) getConnection()).getZookeeper().exists(path, true)));
      }
    } catch (ZkNoNodeException e) {
      // Do nothing, this is what we want as this is not going to leak watch in ZooKeeepr server.
      LOG.info("zkclient {}, watchForData path not existing: {} ", _uid, path);
      return false;
    }
    return true;
  }

  /**
   * Installs a child watch for the given path.
   * @param path
   * @return the current children of the path or null if the zk node with the given path doesn't
   *         exist.
   */
  public List<String> watchForChilds(final String path) {
    return watchForChilds(path, false);
  }

  /**
   *  The following captures about how we reason Zookeeper watch leakage issue based on various
   *  comments in review
   *  1. Removal of a parent zk path (such as currentstate/sessionid) is async to all threads in
   *  Helix router or controller which watches the path. Thus, if we install a watch to a path
   *  expected to be created, we always have the risk of leaking if the path changed.
   *
   *  2. Current the CallbackHandler life cycle is like this:
   *  CallbackHandler for currentstate and some others can be created before the parent path is
   *  created. Thus, we still needs exists() call. This corresponds to INIT change type of
   *  CallbackHanlder. This is the time eventually watchForChilds() with be called with
   *  skipWatchingNonExistNode as false.
   *  Aside from creation time, CallbackHandler normal cycle would see CALLBACK change type. This
   *  time we should normally expected the parent path is created. Thus, the subscription from
   *  CallbackHandler would use skipWatchingNonExistNode false. Avoid leaking path.
   *  Note, if the path is removed, CallbackHandler would see children of parent path as null. THis
   *  would end the CallbackHanlder' life.
   *
   *  From the above life cycle of Callbackhandler, we know the only place that can leak is that
   *  INIT change type time, participant expires the session more than twice in a row before the
   *  watchForChild(skipWatchingNonExistNode=false) issue exists() call.
   *
   *  THe chance of this sequence is slim though.
   *
   */
  private List<String> watchForChilds(final String path, boolean skipWatchingNonExistNode) {
    if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
      throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
    }
    return retryUntilConnected(new Callable<List<String>>() {
      @Override
      public List<String> call() throws Exception {
        if (!skipWatchingNonExistNode) {
          exists(path, true);
        }
        try {
          return getChildren(path, true);
        } catch (ZkNoNodeException e) {
          // ignore, the "exists" watch will listen for the parent node to appear
          LOG.info("zkclient{} watchForChilds path not existing:{} skipWatchingNodeNoteExist: {}",
              _uid, path, skipWatchingNonExistNode);
        }
        return null;
      }
    });
  }

  /**
   * Add authentication information to the connection. This will be used to identify the user and
   * check access to
   * nodes protected by ACLs
   * @param scheme
   * @param auth
   */
  public void addAuthInfo(final String scheme, final byte[] auth) {
    retryUntilConnected(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        getConnection().addAuthInfo(scheme, auth);
        return null;
      }
    });
  }

  /**
   * Connect to ZooKeeper.
   * @param maxMsToWaitUntilConnected
   * @param watcher
   * @throws ZkInterruptedException
   *           if the connection timed out due to thread interruption
   * @throws ZkTimeoutException
   *           if the connection timed out
   * @throws IllegalStateException
   *           if the connection timed out due to thread interruption
   */
  public void connect(final long maxMsToWaitUntilConnected, Watcher watcher)
      throws ZkInterruptedException, ZkTimeoutException, IllegalStateException {
    if (isClosed()) {
      throw new IllegalStateException("ZkClient already closed!");
    }
    boolean started = false;
    acquireEventLock();
    try {
      setShutdownTrigger(false);

      IZkConnection zkConnection = getConnection();
      _eventThread = new ZkEventThread(zkConnection.getServers());

      if (_monitor != null) {
        boolean result = _monitor.setAndInitZkEventThreadMonitor(_eventThread);
        if (!result) {
          LOG.error("register _eventThread monitor failed due to an existing one");
        }
      }

      _eventThread.start();

      LOG.debug("ZkClient {},  _eventThread {}", _uid, _eventThread.getId());

      if (isManagingZkConnection()) {
        zkConnection.connect(watcher);
        LOG.debug("zkclient{} Awaiting connection to Zookeeper server", _uid);
        if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS)) {
          throw new ZkTimeoutException(
              "Unable to connect to zookeeper server within timeout: " + maxMsToWaitUntilConnected);
        }
      } else {
        // if the client is not managing connection, the input connection is supposed to connect.
        if (isConnectionClosed()) {
          throw new ZkClientException(
              "Unable to connect to zookeeper server with the specified ZkConnection");
        }
        // TODO Refine the init state here. Here we pre-config it to be connected. This may not be
        // the case, if the connection is connecting or recovering. -- JJ
        // For shared client, the event notification will not be forwarded before wather add to the
        // connection manager.
        setCurrentState(KeeperState.SyncConnected);
      }

      started = true;
    } finally {
      getEventLock().unlock();

      // we should close the zookeeper instance, otherwise it would keep
      // on trying to connect
      if (!started) {
        close();
      }
    }
  }

  public long getCreationTime(String path) {
    acquireEventLock();
    try {
      return getConnection().getCreateTime(path);
    } catch (KeeperException e) {
      throw ZkException.create(e);
    } catch (InterruptedException e) {
      throw new ZkInterruptedException(e);
    } finally {
      getEventLock().unlock();
    }
  }

  public String getServers() {
    return getConnection().getServers();
  }

  /**
   * Close the client.
   * @throws ZkInterruptedException
   */
  public void close() throws ZkInterruptedException {
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("Closing a zkclient uid:{}, callStack: {} ", _uid, Arrays.asList(calls));
    }
    getEventLock().lock();
    IZkConnection connection = getConnection();
    try {
      if (connection == null || _closed) {
        return;
      }
      setShutdownTrigger(true);
      if (_asyncCallRetryThread != null) {
        _asyncCallRetryThread.interrupt();
        _asyncCallRetryThread.join(2000);
      }
      _eventThread.interrupt();
      _eventThread.join(2000);
      if (isManagingZkConnection()) {
        LOG.info("Closing zkclient uid:{}, zk:{}", _uid, ((ZkConnection) connection).getZookeeper());
        connection.close();
      }
      _closed = true;

      // send state change notification to unlock any wait
      setCurrentState(null);
      getEventLock().getStateChangedCondition().signalAll();
    } catch (InterruptedException e) {
      /**
       * Workaround for HELIX-264: calling ZkClient#close() in its own eventThread context will
       * throw ZkInterruptedException and skip ZkConnection#close()
       */
      if (connection != null) {
        try {
          /**
           * ZkInterruptedException#construct() honors InterruptedException by calling
           * Thread.currentThread().interrupt(); clear it first, so we can safely close the
           * zk-connection
           */
          Thread.interrupted();
          if (isManagingZkConnection()) {
            connection.close();
          }
          /**
           * restore interrupted status of current thread
           */
          Thread.currentThread().interrupt();
        } catch (InterruptedException e1) {
          throw new ZkInterruptedException(e1);
        }
      }
    } finally {
      getEventLock().unlock();
      if (_monitor != null) {
        _monitor.unregister();
      }
      LOG.info("Closed zkclient with uid:{}", _uid);
    }
  }

  public boolean isClosed() {
    try {
      getEventLock().lock();
      return _closed;
    } finally {
      getEventLock().unlock();
    }
  }

  public boolean isConnectionClosed() {
    IZkConnection connection = getConnection();
    return (connection == null || connection.getZookeeperState() == null || !connection
        .getZookeeperState().isAlive());
  }

  public void setShutdownTrigger(boolean triggerState) {
    _shutdownTriggered = triggerState;
  }

  public boolean getShutdownTrigger() {
    return _shutdownTriggered;
  }

  public int numberOfListeners() {
    int listeners = 0;
    for (Set<IZkChildListener> childListeners : _childListener.values()) {
      listeners += childListeners.size();
    }
    for (Set<IZkDataListenerEntry> dataListeners : _dataListener.values()) {
      listeners += dataListeners.size();
    }
    listeners += _stateListener.size();

    return listeners;
  }

  public List<OpResult> multi(final Iterable<Op> ops) throws ZkException {
    if (ops == null) {
      throw new NullPointerException("ops must not be null.");
    }

    return retryUntilConnected(new Callable<List<OpResult>>() {

      @Override
      public List<OpResult> call() throws Exception {
        return getConnection().multi(ops);
      }
    });
  }

  /**
   * @return true if this ZkClient is managing the ZkConnection.
   */
  protected boolean isManagingZkConnection() {
    return true;
  }

  public long getSessionId() {
    ZkConnection zkConnection = ((ZkConnection) getConnection());
    ZooKeeper zk = zkConnection.getZookeeper();
    if (zk == null) {
      throw new ZkClientException(
          "ZooKeeper connection information is not available now. ZkClient might be disconnected.");
    } else {
      return zkConnection.getZookeeper().getSessionId();
    }
  }

  /*
   * Gets a session id in hexadecimal notation.
   * Ex. 1000a5ceb930004 is returned.
   */
  private String getHexSessionId() {
    return Long.toHexString(getSessionId());
  }

  /*
   * Gets the zookeeper instance that ensures its session ID matches the expected session ID.
   * It is used for write operations that suppose the znode to be created by the expected session.
   */
  private ZooKeeper getExpectedZookeeper(final String expectedSessionId) {
    /*
     * Cache the zookeeper reference and make sure later zooKeeper.create() is being run
     * under this zookeeper connection. This is to avoid zk session change after expected
     * session check.
     */
    ZooKeeper zk = ((ZkConnection) getConnection()).getZookeeper();

    /*
     * The operation is NOT session aware, we will use the actual zookeeper session without
     * checking expected session.
     */
    if (expectedSessionId == null || expectedSessionId.isEmpty()) {
      return zk;
    }

    /*
     * If operation is session aware (expectedSession is valid),
     * we have to check whether or not the passed-in(expected) session id
     * matches actual session's id.
     * If not, we should not return a zk object for the zk operation.
     */
    final String actualSessionId = Long.toHexString(zk.getSessionId());
    if (!actualSessionId.equals(expectedSessionId)) {
      throw new ZkSessionMismatchedException(
          "Failed to get expected zookeeper instance! There is a session id mismatch. Expected: "
              + expectedSessionId + ". Actual: " + actualSessionId);
    }

    return zk;
  }

  private String parseExpectedSessionId(Object data) {
    if (!(data instanceof SessionAwareZNRecord)) {
      return null;
    }
    return ((SessionAwareZNRecord) data).getExpectedSessionId();
  }

  // operations to update monitor's counters
  private void record(String path, byte[] data, long startTimeMilliSec,
      ZkClientMonitor.AccessType accessType) {
    if (_monitor != null) {
      int dataSize = (data != null) ? data.length : 0;
      _monitor.record(path, dataSize, startTimeMilliSec, accessType);

      if (GZipCompressionUtil.isCompressed(data)) {
        _monitor.increaseZnodeCompressCounter();
      }
    }
  }

  private void recordFailure(String path, ZkClientMonitor.AccessType accessType) {
    if (_monitor != null) {
      _monitor.recordFailure(path, accessType);
    }
  }

  private void recordStateChange(boolean stateChanged, boolean dataChanged, boolean sessionExpired) {
    // update state change counter.
    if (_monitor != null) {
      if (stateChanged) {
        _monitor.increaseStateChangeEventCounter();
      }
      if (dataChanged) {
        _monitor.increaseDataChangeEventCounter();
      }
      if (sessionExpired) {
        _monitor.increasExpiredSessionCounter();
      }
    }
  }

  /**
   * Creates a {@link IZkStateListener} that wraps a default
   * implementation of {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener}, which means the returned
   * listener runs the methods of {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener}.
   * This is for backward compatibility with {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener}.
   */
  private static class IZkStateListenerI0ItecImpl implements IZkStateListener {
    private org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener _listener;

    IZkStateListenerI0ItecImpl(
        org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener) {
      _listener = listener;
    }

    @Override
    public void handleStateChanged(KeeperState keeperState) throws Exception {
      _listener.handleStateChanged(keeperState);
    }

    @Override
    public void handleNewSession(final String sessionId) throws Exception {
      /*
       * org.I0Itec.zkclient.IZkStateListener does not have handleNewSession(sessionId),
       * so just call handleNewSession() by default.
       */
      _listener.handleNewSession();
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) throws Exception {
      _listener.handleSessionEstablishmentError(error);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof IZkStateListenerI0ItecImpl)) {
        return false;
      }
      if (_listener == null) {
        return false;
      }

      IZkStateListenerI0ItecImpl defaultListener = (IZkStateListenerI0ItecImpl) obj;

      return _listener.equals(defaultListener._listener);
    }

    @Override
    public int hashCode() {
      /*
       * The original listener's hashcode helps find the wrapped listener with the same original
       * listener. This is helpful in unsubscribeStateChanges(listener) when finding the listener
       * to remove.
       */
      return _listener.hashCode();
    }
  }

  private void validateCurrentThread() {
    if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
      throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
    }
  }

  private void checkNumChildrenLimit(String path) throws KeeperException {
    Stat stat = getStat(path);
    if (stat == null) {
      return;
    }

    if (stat.getNumChildren() > NUM_CHILDREN_LIMIT) {
      LOG.error("Failed to get children for path {} because of connection loss. "
              + "Number of children {} exceeds limit {}, aborting retry.", path, stat.getNumChildren(),
          NUM_CHILDREN_LIMIT);
      // MarshallingErrorException could represent transport error: exceeding the
      // Jute buffer size. So use it to exit retry loop and tell that zk is not able to
      // transport the data because packet length is too large.
      throw new KeeperException.MarshallingErrorException();
    } else {
      LOG.debug("Number of children {} is less than limit {}, not exiting retry.",
          stat.getNumChildren(), NUM_CHILDREN_LIMIT);
    }
  }

  private void validateWriteSizeLimitConfig() {
    int serializerSize = ZNRecordUtil.getSerializerWriteSizeLimit();
    LOG.info("ZNRecord serializer write size limit: {}; ZkClient write size limit: {}",
        serializerSize, WRITE_SIZE_LIMIT);
    if (serializerSize > WRITE_SIZE_LIMIT) {
      throw new IllegalStateException("ZNRecord serializer write size limit " + serializerSize
          + " is greater than ZkClient size limit " + WRITE_SIZE_LIMIT);
    }
  }
}
