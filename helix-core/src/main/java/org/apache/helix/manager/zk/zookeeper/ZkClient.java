/**
 * Copyright 2010 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.helix.manager.zk.zookeeper;

import javax.management.JMException;
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

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkLock;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.manager.zk.BasicZkSerializer;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.manager.zk.ZkAsyncCallbacks;
import org.apache.helix.manager.zk.zookeeper.ZkEventThread.ZkEvent;
import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.helix.util.ExponentialBackoffStrategy;
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
 * Abstracts the interaction with zookeeper and allows permanent (not just one time) watches on nodes in ZooKeeper.
 * WARN: Do not use this class directly, use {@link org.apache.helix.manager.zk.ZkClient} instead.
 */
public class ZkClient implements Watcher {
  private static Logger LOG = LoggerFactory.getLogger(ZkClient.class);
  private static long MAX_RECONNECT_INTERVAL_MS = 30000; // 30 seconds

  private final IZkConnection _connection;
  private final long _operationRetryTimeoutInMillis;
  private final Map<String, Set<IZkChildListener>> _childListener =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<IZkDataListenerEntry>> _dataListener =
      new ConcurrentHashMap<>();
  private final Set<IZkStateListener> _stateListener = new CopyOnWriteArraySet<>();
  private KeeperState _currentState;
  private final ZkLock _zkEventLock = new ZkLock();
  private boolean _shutdownTriggered;
  private ZkEventThread _eventThread;
  // TODO PVo remove this later
  private Thread _zookeeperEventThread;
  private volatile boolean _closed;
  private PathBasedZkSerializer _pathBasedZkSerializer;
  private ZkClientMonitor _monitor;

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
        } // else, the node was updated again after the notification. Propagation latency is unavailable.
      }
    }
  }


  protected ZkClient(IZkConnection zkConnection, int connectionTimeout, long operationRetryTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      String monitorInstanceName, boolean monitorRootPathOnly) {
    if (zkConnection == null) {
      throw new NullPointerException("Zookeeper connection is null!");
    }
    _connection = zkConnection;
    _pathBasedZkSerializer = zkSerializer;
    _operationRetryTimeoutInMillis = operationRetryTimeout;

    connect(connectionTimeout, this);

    // initiate monitor
    try {
      if (monitorKey != null && !monitorKey.isEmpty() && monitorType != null && !monitorType
          .isEmpty()) {
        _monitor =
            new ZkClientMonitor(monitorType, monitorKey, monitorInstanceName, monitorRootPathOnly,
                _eventThread);
        _monitor.register();
      } else {
        LOG.info("ZkClient monitor key or type is not provided. Skip monitoring.");
      }
    } catch (JMException e) {
      LOG.error("Error in creating ZkClientMonitor", e);
    }
  }

  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    synchronized (_childListener) {
      Set<IZkChildListener> listeners = _childListener.get(path);
      if (listeners == null) {
        listeners = new CopyOnWriteArraySet<>();
        _childListener.put(path, listeners);
      }
      listeners.add(listener);
    }
    return watchForChilds(path);
  }

  public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
    synchronized (_childListener) {
      final Set<IZkChildListener> listeners = _childListener.get(path);
      if (listeners != null) {
        listeners.remove(childListener);
      }
    }
  }

  public void subscribeDataChanges(String path, IZkDataListener listener) {
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
          LOG.debug(
              "Subscribed data changes for " + path + ", listener: " + listener + ", prefetch data: "
                  + prefetchEnabled);
        }
      }
    }
    watchForData(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Subscribed data changes for " + path);
    }
  }

  private boolean isPrefetchEnabled(IZkDataListener dataListener) {
    PreFetch preFetch = dataListener.getClass().getAnnotation(PreFetch.class);
    if (preFetch != null) {
      return preFetch.enabled();
    }

    Method callbackMethod = IZkDataListener.class.getMethods()[0];
    try {
      Method method = dataListener.getClass()
          .getMethod(callbackMethod.getName(), callbackMethod.getParameterTypes());
      PreFetch preFetchInMethod = method.getAnnotation(PreFetch.class);
      if (preFetchInMethod != null) {
        return preFetchInMethod.enabled();
      }
    } catch (NoSuchMethodException e) {
      LOG.warn(
          "No method " + callbackMethod.getName() + " defined in listener " + dataListener.getClass()
              .getCanonicalName());
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

  public void unsubscribeStateChanges(IZkStateListener stateListener) {
    synchronized (_stateListener) {
      _stateListener.remove(stateListener);
    }
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
   *
   * @param path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createPersistent(String path)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createPersistent(path, false);
  }

  /**
   * Create a persistent node and set its ACLs.
   *
   * @param path
   * @param createParents
   *            if true all parent dirs are created as well and no {@link ZkNodeExistsException} is thrown in case the
   *            path already exists
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createPersistent(String path, boolean createParents)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    createPersistent(path, createParents, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  /**
   * Create a persistent node and set its ACLs.
   *
   * @param path
   * @param acl
   *            List of ACL permissions to assign to the node
   * @param createParents
   *            if true all parent dirs are created as well and no {@link ZkNodeExistsException} is thrown in case the
   *            path already exists
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
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
   * Create a persistent node.
   *
   * @param path
   * @param data
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createPersistent(String path, Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Create a persistent node.
   *
   * @param path
   * @param data
   * @param acl
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createPersistent(String path, Object data, List<ACL> acl) {
    create(path, data, acl, CreateMode.PERSISTENT);
  }

  /**
   * Create a persistent, sequental node.
   *
   * @param path
   * @param data
   * @return create node's path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public String createPersistentSequential(String path, Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  /**
   * Create a persistent, sequential node and set its ACL.
   *
   * @param path
   * @param acl
   * @param data
   * @return create node's path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public String createPersistentSequential(String path, Object data, List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  /**
   * Create an ephemeral node.
   *
   * @param path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createEphemeral(final String path)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, null, CreateMode.EPHEMERAL);
  }

  /**
   * Create an ephemeral node and set its ACL.
   *
   * @param path
   * @param acl
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createEphemeral(final String path, final List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, null, acl, CreateMode.EPHEMERAL);
  }

  /**
   * Create a node.
   *
   * @param path
   * @param data
   * @param mode
   * @return create node's path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public String create(final String path, Object data, final CreateMode mode)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
  }

  /**
   * Create a node with ACL.
   *
   * @param path
   * @param datat
   * @param acl
   * @param mode
   * @return create node's path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public String create(final String path, Object datat, final List<ACL> acl, final CreateMode mode)
      throws IllegalArgumentException, ZkException {
    if (path == null) {
      throw new NullPointerException("Path must not be null.");
    }
    if (acl == null || acl.size() == 0) {
      throw new NullPointerException("Missing value for ACL");
    }
    long startT = System.currentTimeMillis();
    try {
      final byte[] data = datat == null ? null : serialize(datat, path);
      checkDataSizeLimit(data);
      String actualPath = retryUntilConnected(new Callable<String>() {
        @Override
        public String call() throws Exception {
          return getConnection().create(path, data, acl, mode);
        }
      });
      record(path, data, startT, ZkClientMonitor.AccessType.WRITE);
      return actualPath;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.WRITE);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("create, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  /**
   * Create an ephemeral node.
   *
   * @param path
   * @param data
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createEphemeral(final String path, final Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, CreateMode.EPHEMERAL);
  }

  /**
   * Create an ephemeral node.
   *
   * @param path
   * @param data
   * @param acl
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public void createEphemeral(final String path, final Object data, final List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    create(path, data, acl, CreateMode.EPHEMERAL);
  }

  /**
   * Create an ephemeral, sequential node.
   *
   * @param path
   * @param data
   * @return created path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public String createEphemeralSequential(final String path, final Object data)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  /**
   * Create an ephemeral, sequential node with ACL.
   *
   * @param path
   * @param data
   * @param acl
   * @return created path
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs
   */
  public String createEphemeralSequential(final String path, final Object data, final List<ACL> acl)
      throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
    return create(path, data, acl, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  @Override
  public void process(WatchedEvent event) {
    long notificationTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received event: " + event);
    }
    _zookeeperEventThread = Thread.currentThread();

    boolean stateChanged = event.getPath() == null;
    boolean znodeChanged = event.getPath() != null;
    boolean dataChanged = event.getType() == Event.EventType.NodeDataChanged
        || event.getType() == Event.EventType.NodeDeleted
        || event.getType() == Event.EventType.NodeCreated
        || event.getType() == Event.EventType.NodeChildrenChanged;


    if (event.getType() == Event.EventType.NodeDeleted) {
      if (LOG.isDebugEnabled()) {
        String path = event.getPath();
        LOG.debug(path);
      }
    }

    getEventLock().lock();
    try {
      // We might have to install child change event listener if a new node was created
      if (getShutdownTrigger()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("ignoring event '{" + event.getType() + " | " + event.getPath()
              + "}' since shutdown triggered");
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

        // If the session expired we have to signal all conditions, because watches might have been removed and
        // there is no guarantee that those
        // conditions will be signaled at all after an Expired event
        // TODO PVo write a test for this
        if (event.getState() == KeeperState.Expired) {
          getEventLock().getZNodeEventCondition().signalAll();
          getEventLock().getDataChangedCondition().signalAll();
          // We also have to notify all listeners that something might have changed
          fireAllEvents();
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
      recordStateChange(stateChanged, dataChanged);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Leaving process event");
      }
    }
  }

  private void fireAllEvents() {
    for (Entry<String, Set<IZkChildListener>> entry : _childListener.entrySet()) {
      fireChildChangedEvents(entry.getKey(), entry.getValue());
    }
    for (Entry<String, Set<IZkDataListenerEntry>> entry : _dataListener.entrySet()) {
      fireDataChangedEvents(entry.getKey(), entry.getValue(), OptionalLong.empty());
    }
  }

  public List<String> getChildren(String path) {
    return getChildren(path, hasListeners(path));
  }

  protected List<String> getChildren(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();
    try {
      List<String> children = retryUntilConnected(new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          return getConnection().getChildren(path, watch);
        }
      });
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      return children;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getChildren, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }


  /**
   * Counts number of children for the given path.
   *
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
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  public Stat getStat(final String path) {
    return getStat(path, false);
  }

  private Stat getStat(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();
    try {
      Stat stat = retryUntilConnected(
          () -> ((ZkConnection) getConnection()).getZookeeper().exists(path, watch));
      record(path, null, startT, ZkClientMonitor.AccessType.READ);
      return stat;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  protected void processStateChanged(WatchedEvent event) {
    LOG.info("zookeeper state changed (" + event.getState() + ")");
    setCurrentState(event.getState());
    if (getShutdownTrigger()) {
      return;
    }
    fireStateChangedEvent(event.getState());
    if (isManagingZkConnection() && event.getState() == KeeperState.Expired) {
      reconnectOnExpiring();
    }
  }

  private void reconnectOnExpiring() {
    int retryCount = 0;
    ExponentialBackoffStrategy retryStrategy =
        new ExponentialBackoffStrategy(MAX_RECONNECT_INTERVAL_MS, true);

    Exception reconnectException = new ZkException("Shutdown triggered.");
    while (!isClosed()) {
      try {
        reconnect();
        fireNewSessionEvents();
        return;
      } catch (ZkInterruptedException interrupt) {
        reconnectException = interrupt;
        break;
      } catch (Exception e) {
        reconnectException = e;
        long waitInterval = retryStrategy.getNextWaitInterval(retryCount++);
        LOG.warn("ZkClient reconnect on expiring failed. Will retry after {} ms", waitInterval, e);
        try {
          Thread.sleep(waitInterval);
        } catch (InterruptedException ex) {
          reconnectException = ex;
          break;
        }
      }
    }

    LOG.info("Unable to re-establish connection. Notifying consumer of the following exception: ",
        reconnectException);
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

  private void fireNewSessionEvents() {
    for (final IZkStateListener stateListener : _stateListener) {
      _eventThread.send(new ZkEvent("New session event sent to " + stateListener) {

        @Override public void run() throws Exception {
          stateListener.handleNewSession();
        }
      });
    }
  }

  protected void fireStateChangedEvent(final KeeperState state) {
    for (final IZkStateListener stateListener : _stateListener) {
      _eventThread.send(new ZkEvent("State changed to " + state + " sent to " + stateListener) {

        @Override public void run() throws Exception {
          stateListener.handleStateChanged(state);
        }
      });
    }
  }

  private void fireSessionEstablishmentError(final Throwable error) {
    for (final IZkStateListener stateListener : _stateListener) {
      _eventThread
          .send(new ZkEvent("Session establishment error(" + error + ") sent to " + stateListener) {

            @Override public void run() throws Exception {
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
    } catch (HelixException e) {
      LOG.error("Failed to recursively delete path " + path, e);
      return false;
    }
  }

  /**
   * Delete the path as well as all its children.
   * @param path
   * @throws HelixException
   */
  public void deleteRecursively(String path) throws HelixException {
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
      LOG.error("Failed to delete " + path, e);
      throw new HelixException("Failed to delete " + path, e);
    }
  }

  private void processDataOrChildChange(WatchedEvent event, long notificationTime) {
    final String path = event.getPath();

    if (event.getType() == EventType.NodeChildrenChanged || event.getType() == EventType.NodeCreated
        || event.getType() == EventType.NodeDeleted) {
      Set<IZkChildListener> childListeners = _childListener.get(path);
      if (childListeners != null && !childListeners.isEmpty()) {
        // TODO recording child changed event propagation latency as well. Note this change will introduce additional ZK access.
        fireChildChangedEvents(path, childListeners);
      }
    }

    if (event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted
        || event.getType() == EventType.NodeCreated) {
      Set<IZkDataListenerEntry> listeners = _dataListener.get(path);
      if (listeners != null && !listeners.isEmpty()) {
        fireDataChangedEvents(event.getPath(), listeners, OptionalLong.of(notificationTime));
      }
    }
  }

  private void fireDataChangedEvents(final String path, Set<IZkDataListenerEntry> listeners,
      final OptionalLong notificationTime) {
    try {
      final ZkPathStatRecord pathStatRecord = new ZkPathStatRecord(path);
      // Trigger listener callbacks
      for (final IZkDataListenerEntry listener : listeners) {
        _eventThread.send(new ZkEvent(
            "Data of " + path + " changed sent to " + listener.getDataListener()
                + " prefetch data: " + listener.isPrefetchData()) {
          @Override
          public void run() throws Exception {
            // Reinstall watch before listener callbacks to check the znode status
            if (!pathStatRecord.pathChecked()) {
              pathStatRecord.recordPathStat(getStat(path, true), notificationTime);
            }
            if (!pathStatRecord.pathExists()) {
              // no znode found at the path, trigger data deleted handler.
              listener.getDataListener().handleDataDeleted(path);
            } else {
              Object data = null;
              if (listener.isPrefetchData()) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Prefetch data for path: {}", path);
                }
                try {
                  data = readData(path, null, true);
                } catch (ZkNoNodeException e) {
                  LOG.warn("Prefetch data for path: {} failed.", path, e);
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
      LOG.error("Failed to fire data changed event for path: {}", path, e);
    }
  }

  private void fireChildChangedEvents(final String path, Set<IZkChildListener> childListeners) {
    try {
      final ZkPathStatRecord pathStatRecord = new ZkPathStatRecord(path);
      for (final IZkChildListener listener : childListeners) {
        _eventThread.send(new ZkEvent("Children of " + path + " changed sent to " + listener) {
          @Override
          public void run() throws Exception {
            // Reinstall watch before listener callbacks to check the znode status
            if (!pathStatRecord.pathChecked()) {
              pathStatRecord.recordPathStat(getStat(path, hasListeners(path)), OptionalLong.empty());
            }
            List<String> children = null;
            if (pathStatRecord.pathExists()) {
              try {
                children = getChildren(path);
              } catch (ZkNoNodeException e) {
                LOG.warn("Get children under path: {} failed.", path, e);
                // Continue trigger the change handler
              }
            }
            listener.handleChildChange(path, children);
          }
        });
      }
    } catch (Exception e) {
      LOG.error("Failed to fire child changed event. Unable to getChildren.", e);
    }
  }

  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time)
      throws ZkInterruptedException {
    Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Waiting until znode '" + path + "' becomes available.");
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

  public boolean waitUntilConnected(long time, TimeUnit timeUnit) throws ZkInterruptedException {
    return waitForKeeperState(KeeperState.SyncConnected, time, timeUnit);
  }

  public boolean waitForKeeperState(KeeperState keeperState, long time, TimeUnit timeUnit)
      throws ZkInterruptedException {
    if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
      throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
    }
    Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

    LOG.debug("Waiting for keeper state " + keeperState);
    acquireEventLock();
    try {
      boolean stillWaiting = true;
      while (_currentState != keeperState) {
        if (!stillWaiting) {
          return false;
        }
        stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeout);
      }
      LOG.debug("State is " + (_currentState == null ? "CLOSED" : _currentState));
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
   *
   * @param <T>
   * @param callable
   * @return result of Callable
   * @throws ZkInterruptedException
   *             if operation was interrupted, or a required reconnection got interrupted
   * @throws IllegalArgumentException
   *             if called from anything except the ZooKeeper event thread
   * @throws ZkException
   *             if any ZooKeeper exception occurred
   * @throws RuntimeException
   *             if any other exception occurs from invoking the Callable
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
          // we give the event thread some time to update the status to 'Disconnected'
          Thread.yield();
          waitForRetry();
        } catch (SessionExpiredException e) {
          // we give the event thread some time to update the status to 'Expired'
          Thread.yield();
          waitForRetry();
        } catch (KeeperException e) {
          throw ZkException.create(e);
        } catch (InterruptedException e) {
          throw new ZkInterruptedException(e);
        } catch (Exception e) {
          throw ExceptionUtil.convertToRuntimeException(e);
        }
        // before attempting a retry, check whether retry timeout has elapsed
        if (System.currentTimeMillis() - operationStartTime > _operationRetryTimeoutInMillis) {
          throw new ZkTimeoutException("Operation cannot be retried because of retry timeout (" + _operationRetryTimeoutInMillis
              + " milli seconds)");
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
   * Returns a mutex all zookeeper events are synchronized aginst. So in case you need to do something without getting
   * any zookeeper event interruption synchronize against this mutex. Also all threads waiting on this mutex object
   * will be notified on an event.
   *
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to delete path " + path + ", znode does not exist!");
        }
      }
      record(path, null, startT, ZkClientMonitor.AccessType.WRITE);
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.WRITE);
      LOG.warn("Failed to delete path " + path + "! " + e);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("delete, path: " + path + ", time: " + (endT - startT) + " ms");
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

  @SuppressWarnings("unchecked") public <T extends Object> T readData(String path) {
    return (T) readData(path, false);
  }

  @SuppressWarnings("unchecked") public <T extends Object> T readData(String path,
      boolean returnNullIfPathNotExists) {
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

        @Override public byte[] call() throws Exception {
          return getConnection().readData(path, stat, watch);
        }
      });
      record(path, data, startT, ZkClientMonitor.AccessType.READ);
      return (T) deserialize(data, path);
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.READ);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getData, path: " + path + ", time: " + (endT - startT) + " ms");
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
   * Updates data of an existing znode. The current content of the znode is passed to the {@link DataUpdater} that is
   * passed into this method, which returns the new content. The new content is only written back to ZooKeeper if
   * nobody has modified the given znode in between. If a concurrent change has been detected the new data of the
   * znode is passed to the updater once again until the new contents can be successfully written back to ZooKeeper.
   *
   * @param <T>
   * @param path
   *            The path of the znode.
   * @param updater
   *            Updater that creates the new contents.
   */
  @SuppressWarnings("unchecked") public <T extends Object> void updateDataSerialized(String path,
      DataUpdater<T> updater) {
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
      checkDataSizeLimit(data);
      final Stat stat = (Stat) retryUntilConnected(new Callable<Object>() {
        @Override public Object call() throws Exception {
          return getConnection().writeDataReturnStat(path, data, expectedVersion);
        }
      });
      record(path, data, startT, ZkClientMonitor.AccessType.WRITE);
      return stat;
    } catch (Exception e) {
      recordFailure(path, ZkClientMonitor.AccessType.WRITE);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  public Stat writeDataGetStat(final String path, Object datat, final int expectedVersion) {
    return writeDataReturnStat(path, datat, expectedVersion);
  }


  public void asyncCreate(final String path, Object datat, final CreateMode mode,
      final ZkAsyncCallbacks.CreateCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data = (datat == null ? null : serialize(datat, path));
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) getConnection()).getZookeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
            // Arrays.asList(DEFAULT_ACL),
            mode, cb, new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT,
                data == null ? 0 : data.length, false));
        return null;
      }
    });
  }

  // Async Data Accessors
  public void asyncSetData(final String path, Object datat, final int version,
      final ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data = serialize(datat, path);
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) getConnection()).getZookeeper().setData(path, data, version, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT,
                data == null ? 0 : data.length, false));
        return null;
      }
    });
  }

  public void asyncGetData(final String path, final ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) getConnection()).getZookeeper().getData(path, null, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT, 0, true));
        return null;
      }
    });
  }

  public void asyncExists(final String path, final ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) getConnection()).getZookeeper().exists(path, null, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT, 0, true));
        return null;
      }
    });
  }

  public void asyncDelete(final String path, final ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) getConnection()).getZookeeper().delete(path, -1, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT, 0, false));
        return null;
      }
    });
  }

  private void checkDataSizeLimit(byte[] data) {
    if (data != null && data.length > ZNRecord.SIZE_LIMIT) {
      LOG.error("Data size larger than 1M, will not write to zk. Data (first 1k): "
          + new String(data).substring(0, 1024));
      throw new HelixException("Data size larger than 1M");
    }
  }

  public void watchForData(final String path) {
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        getConnection().exists(path, true);
        return null;
      }
    });
  }

  /**
   * Installs a child watch for the given path.
   *
   * @param path
   * @return the current children of the path or null if the zk node with the given path doesn't exist.
   */
  public List<String> watchForChilds(final String path) {
    if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
      throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
    }
    return retryUntilConnected(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        exists(path, true);
        try {
          return getChildren(path, true);
        } catch (ZkNoNodeException e) {
          // ignore, the "exists" watch will listen for the parent node to appear
        }
        return null;
      }
    });
  }

  /**
   * Add authentication information to the connection. This will be used to identify the user and check access to
   * nodes protected by ACLs
   *
   * @param scheme
   * @param auth
   */
  public void addAuthInfo(final String scheme, final byte[] auth) {
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        getConnection().addAuthInfo(scheme, auth);
        return null;
      }
    });
  }

  /**
   * Connect to ZooKeeper.
   *
   * @param maxMsToWaitUntilConnected
   * @param watcher
   * @throws ZkInterruptedException
   *             if the connection timed out due to thread interruption
   * @throws ZkTimeoutException
   *             if the connection timed out
   * @throws IllegalStateException
   *             if the connection timed out due to thread interruption
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
      _eventThread.start();

      if (isManagingZkConnection()) {
        zkConnection.connect(watcher);
        LOG.debug("Awaiting connection to Zookeeper server");
        if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS)) {
          throw new ZkTimeoutException(
              "Unable to connect to zookeeper server within timeout: " + maxMsToWaitUntilConnected);
        }
      } else {
        // if the client is not managing connection, the input connection is supposed to connect.
        if (isConnectionClosed()) {
          throw new HelixException(
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
   *
   * @throws ZkInterruptedException
   */
  public void close() throws ZkInterruptedException {
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("closing a zkclient. callStack: " + Arrays.asList(calls));
    }
    getEventLock().lock();
    IZkConnection connection = getConnection();
    try {
      if (connection == null || _closed) {
        return;
      }
      setShutdownTrigger(true);
      _eventThread.interrupt();
      _eventThread.join(2000);
      if (isManagingZkConnection()) {
        LOG.info("Closing zkclient: " + ((ZkConnection) connection).getZookeeper());
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
      LOG.info("Closed zkclient");
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
    return (connection == null || connection.getZookeeperState() == null ||
        !connection.getZookeeperState().isAlive());
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

      @Override public List<OpResult> call() throws Exception {
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
      throw new HelixException(
          "ZooKeeper connection information is not available now. ZkClient might be disconnected.");
    } else {
      return zkConnection.getZookeeper().getSessionId();
    }
  }

  // operations to update monitor's counters
  private void record(String path, byte[] data, long startTimeMilliSec, ZkClientMonitor.AccessType accessType) {
    if (_monitor != null) {
      int dataSize = (data != null) ? data.length : 0;
      _monitor.record(path, dataSize, startTimeMilliSec, accessType);
    }
  }

  private void recordFailure(String path, ZkClientMonitor.AccessType accessType) {
    if (_monitor != null) {
      _monitor.recordFailure(path, accessType);
    }
  }

  private void recordStateChange(boolean stateChanged, boolean dataChanged) {
    // update state change counter.
    if (_monitor != null) {
      if (stateChanged) {
        _monitor.increaseStateChangeEventCounter();
      }
      if (dataChanged) {
        _monitor.increaseDataChangeEventCounter();
      }
    }
  }
}
