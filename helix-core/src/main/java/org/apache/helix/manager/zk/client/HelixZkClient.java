package org.apache.helix.manager.zk.client;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.manager.zk.BasicZkSerializer;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.manager.zk.ZkAsyncCallbacks;
import org.apache.helix.manager.zk.zookeeper.IZkStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Helix ZkClient interface.
 */
public interface HelixZkClient {
  int DEFAULT_OPERATION_TIMEOUT = Integer.MAX_VALUE;
  int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  int DEFAULT_SESSION_TIMEOUT = 30 * 1000;

  // listener subscription
  List<String> subscribeChildChanges(String path, IZkChildListener listener);

  void unsubscribeChildChanges(String path, IZkChildListener listener);

  void subscribeDataChanges(String path, IZkDataListener listener);

  void unsubscribeDataChanges(String path, IZkDataListener listener);

  /*
   * This is for backwards compatibility.
   *
   * TODO: remove below default implementation when getting rid of I0Itec in the new zk client.
   */
  default void subscribeStateChanges(final IZkStateListener listener) {
    subscribeStateChanges(new DefaultI0ItecIZkStateListener(listener));
  }

  /*
   * This is for backwards compatibility.
   *
   * TODO: remove below default implementation when getting rid of I0Itec in the new zk client.
   */
  default void unsubscribeStateChanges(IZkStateListener listener) {
    unsubscribeStateChanges(new DefaultI0ItecIZkStateListener(listener));
  }

  /**
   * Subscribes state changes for a {@link org.I0Itec.zkclient.IZkStateListener} listener.
   *
   * @deprecated
   * This is deprecated. It is kept for backwards compatibility. Please use
   * {@link #subscribeStateChanges(org.apache.helix.manager.zk.zookeeper.IZkStateListener)}.
   *
   * @param listener {@link org.I0Itec.zkclient.IZkStateListener} listener
   */
  @Deprecated
  void subscribeStateChanges(final org.I0Itec.zkclient.IZkStateListener listener);

  /**
   * Unsubscribes state changes for a {@link org.I0Itec.zkclient.IZkStateListener} listener.
   *
   * @deprecated
   * This is deprecated. It is kept for backwards compatibility. Please use
   * {@link #unsubscribeStateChanges(org.apache.helix.manager.zk.zookeeper.IZkStateListener)}.
   *
   * @param listener {@link org.I0Itec.zkclient.IZkStateListener} listener
   */
  @Deprecated
  void unsubscribeStateChanges(org.I0Itec.zkclient.IZkStateListener listener);

  void unsubscribeAll();

  // data access
  void createPersistent(String path);

  void createPersistent(String path, boolean createParents);

  void createPersistent(String path, boolean createParents, List<ACL> acl);

  void createPersistent(String path, Object data);

  void createPersistent(String path, Object data, List<ACL> acl);

  String createPersistentSequential(String path, Object data);

  String createPersistentSequential(String path, Object data, List<ACL> acl);

  void createEphemeral(final String path);

  void createEphemeral(final String path, final List<ACL> acl);

  String create(final String path, Object data, final CreateMode mode);

  String create(final String path, Object datat, final List<ACL> acl, final CreateMode mode);

  void createEphemeral(final String path, final Object data);

  void createEphemeral(final String path, final Object data, final List<ACL> acl);

  String createEphemeralSequential(final String path, final Object data);

  String createEphemeralSequential(final String path, final Object data, final List<ACL> acl);

  List<String> getChildren(String path);

  int countChildren(String path);

  boolean exists(final String path);

  Stat getStat(final String path);

  boolean waitUntilExists(String path, TimeUnit timeUnit, long time);

  void deleteRecursively(String path);

  boolean delete(final String path);

  <T extends Object> T readData(String path);

  <T extends Object> T readData(String path, boolean returnNullIfPathNotExists);

  <T extends Object> T readData(String path, Stat stat);

  <T extends Object> T readData(final String path, final Stat stat, final boolean watch);

  <T extends Object> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists);

  void writeData(String path, Object object);

  <T extends Object> void updateDataSerialized(String path, DataUpdater<T> updater);

  void writeData(final String path, Object datat, final int expectedVersion);

  Stat writeDataReturnStat(final String path, Object datat, final int expectedVersion);

  Stat writeDataGetStat(final String path, Object datat, final int expectedVersion);

  void asyncCreate(final String path, Object datat, final CreateMode mode,
      final ZkAsyncCallbacks.CreateCallbackHandler cb);

  void asyncSetData(final String path, Object datat, final int version,
      final ZkAsyncCallbacks.SetDataCallbackHandler cb);

  void asyncGetData(final String path, final ZkAsyncCallbacks.GetDataCallbackHandler cb);

  void asyncExists(final String path, final ZkAsyncCallbacks.ExistsCallbackHandler cb);

  void asyncDelete(final String path, final ZkAsyncCallbacks.DeleteCallbackHandler cb);

  void watchForData(final String path);

  List<String> watchForChilds(final String path);

  long getCreationTime(String path);

  List<OpResult> multi(final Iterable<Op> ops);

  // ZK state control
  boolean waitUntilConnected(long time, TimeUnit timeUnit);

  String getServers();

  long getSessionId();

  void close();

  boolean isClosed();

  // other
  byte[] serialize(Object data, String path);

  <T extends Object> T deserialize(byte[] data, String path);

  void setZkSerializer(ZkSerializer zkSerializer);

  void setZkSerializer(PathBasedZkSerializer zkSerializer);

  PathBasedZkSerializer getZkSerializer();

  /**
   * A class that wraps a default implementation of
   * {@link org.apache.helix.manager.zk.zookeeper.IZkStateListener}, which means this listener
   * runs the methods of {@link org.apache.helix.manager.zk.zookeeper.IZkStateListener}.
   * This is for backward compatibility and to avoid breaking the original implementation of
   * {@link org.I0Itec.zkclient.IZkStateListener}.
   */
  class DefaultI0ItecIZkStateListener implements org.I0Itec.zkclient.IZkStateListener {
    private IZkStateListener listener;

    DefaultI0ItecIZkStateListener(IZkStateListener listener) {
      this.listener = listener;
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
      listener.handleStateChanged(keeperState);
    }

    @Override
    public void handleNewSession() throws Exception {
      /*
       * org.apache.helix.manager.zk.zookeeper.IZkStateListener does not have handleNewSession(),
       * so null is passed into handleNewSession(sessionId).
       */
      listener.handleNewSession(null);
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) throws Exception {
      listener.handleSessionEstablishmentError(error);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof IZkStateListener)) {
        return false;
      }

      DefaultI0ItecIZkStateListener defaultListener = (DefaultI0ItecIZkStateListener) obj;

      return listener.equals(defaultListener.listener);
    }

    @Override
    public int hashCode() {
      /*
       * The original listener's hashcode helps find the wrapped listener with the same original
       * listener. This is helpful in unsubscribeStateChanges(listener).
       */
      return listener.hashCode();
    }
  }

  /**
   * Configuration for creating a new ZkConnection.
   */
  class ZkConnectionConfig {
    // Connection configs
    private final String _zkServers;
    private int _sessionTimeout = HelixZkClient.DEFAULT_SESSION_TIMEOUT;

    public ZkConnectionConfig(String zkServers) {
      _zkServers = zkServers;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof ZkConnectionConfig)) {
        return false;
      }
      ZkConnectionConfig configObj = (ZkConnectionConfig) obj;
      return (_zkServers == null && configObj._zkServers == null ||
          _zkServers != null && _zkServers.equals(configObj._zkServers)) &&
          _sessionTimeout == configObj._sessionTimeout;
    }

    @Override
    public int hashCode() {
      return _sessionTimeout * 31 + _zkServers.hashCode();
    }

    @Override
    public String toString() {
      return (_zkServers + "_" + _sessionTimeout).replaceAll("[\\W]", "_");
    }

    public ZkConnectionConfig setSessionTimeout(Integer sessionTimeout) {
      this._sessionTimeout = sessionTimeout;
      return this;
    }

    public String getZkServers() {
      return _zkServers;
    }

    public int getSessionTimeout() {
      return _sessionTimeout;
    }
  }

  /**
   * Configuration for creating a new ZkClient with serializer and monitor.
   */
  class ZkClientConfig {
    // For client to init the connection
    private long _connectInitTimeout = HelixZkClient.DEFAULT_CONNECTION_TIMEOUT;

    // Data access configs
    private long _operationRetryTimeout = HelixZkClient.DEFAULT_OPERATION_TIMEOUT;

    // Others
    private PathBasedZkSerializer _zkSerializer;

    // Monitoring
    private String _monitorType;
    private String _monitorKey;
    private String _monitorInstanceName = null;
    private boolean _monitorRootPathOnly = true;

    public ZkClientConfig setZkSerializer(PathBasedZkSerializer zkSerializer) {
      this._zkSerializer = zkSerializer;
      return this;
    }

    public ZkClientConfig setZkSerializer(ZkSerializer zkSerializer) {
      this._zkSerializer = new BasicZkSerializer(zkSerializer);
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorType
     */
    public ZkClientConfig setMonitorType(String monitorType) {
      this._monitorType = monitorType;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorKey
     */
    public ZkClientConfig setMonitorKey(String monitorKey) {
      this._monitorKey = monitorKey;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is optional.
     *
     * @param instanceName
     */
    public ZkClientConfig setMonitorInstanceName(String instanceName) {
      this._monitorInstanceName = instanceName;
      return this;
    }

    public ZkClientConfig setMonitorRootPathOnly(Boolean monitorRootPathOnly) {
      this._monitorRootPathOnly = monitorRootPathOnly;
      return this;
    }

    public ZkClientConfig setOperationRetryTimeout(Long operationRetryTimeout) {
      this._operationRetryTimeout = operationRetryTimeout;
      return this;
    }

    public ZkClientConfig setConnectInitTimeout(long _connectInitTimeout) {
      this._connectInitTimeout = _connectInitTimeout;
      return this;
    }

    public PathBasedZkSerializer getZkSerializer() {
      if (_zkSerializer == null) {
        _zkSerializer = new BasicZkSerializer(new SerializableSerializer());
      }
      return _zkSerializer;
    }

    public long getOperationRetryTimeout() {
      return _operationRetryTimeout;
    }

    public String getMonitorType() {
      return _monitorType;
    }

    public String getMonitorKey() {
      return _monitorKey;
    }

    public String getMonitorInstanceName() {
      return _monitorInstanceName;
    }

    public boolean isMonitorRootPathOnly() {
      return _monitorRootPathOnly;
    }

    public long getConnectInitTimeout() {
      return _connectInitTimeout;
    }
  }
}
