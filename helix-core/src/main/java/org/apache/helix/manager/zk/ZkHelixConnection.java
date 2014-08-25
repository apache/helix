package org.apache.helix.manager.zk;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixConnectionStateListener;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixController;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.HelixMultiClusterController;
import org.apache.helix.HelixParticipant;
import org.apache.helix.HelixRole;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.MessageListener;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.AutoFallbackPropertyStore;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZkHelixConnection implements HelixConnection, IZkStateListener {
  private static Logger LOG = Logger.getLogger(ZkHelixConnection.class);

  private final String _zkAddr;
  private final int _sessionTimeout;
  private SessionId _sessionId;
  ZkClient _zkclient; // keep it package level for test purpose
  private BaseDataAccessor<ZNRecord> _baseAccessor;
  private ConfigAccessor _configAccessor;
  private final Set<HelixConnectionStateListener> _connectionListener;

  final Map<HelixRole, List<ZkCallbackHandler>> _handlers; // keep it package level for test purpose
  private final HelixManagerProperties _properties;

  /**
   * Keep track of timestamps that zk State has become Disconnected
   * If in a _timeWindowLengthMs window zk State has become Disconnected
   * for more than_maxDisconnectThreshold times disconnect the zkHelixManager
   */
  private final List<Long> _disconnectTimeHistory = new ArrayList<Long>();
  private final int _flappingTimeWindowMs;
  private final int _maxDisconnectThreshold;

  /**
   * helix version#
   */
  private final String _version;

  public ZkHelixConnection(String zkAddr) {
    _zkAddr = zkAddr;
    _handlers = new HashMap<HelixRole, List<ZkCallbackHandler>>();
    _connectionListener = new CopyOnWriteArraySet<HelixConnectionStateListener>();

    /**
     * use system property if available
     */
    _flappingTimeWindowMs =
        getSystemPropertyAsInt("helixmanager.flappingTimeWindow",
            ZKHelixManager.FLAPPING_TIME_WINDIOW);

    _maxDisconnectThreshold =
        getSystemPropertyAsInt("helixmanager.maxDisconnectThreshold",
            ZKHelixManager.MAX_DISCONNECT_THRESHOLD);

    _sessionTimeout =
        getSystemPropertyAsInt("zk.session.timeout", ZkClient.DEFAULT_SESSION_TIMEOUT);

    _properties = new HelixManagerProperties("cluster-manager-version.properties");
    _version = _properties.getVersion();

  }

  private int getSystemPropertyAsInt(String propertyKey, int propertyDefaultValue) {
    String valueString = System.getProperty(propertyKey, "" + propertyDefaultValue);

    try {
      int value = Integer.parseInt(valueString);
      if (value > 0) {
        return value;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Exception while parsing property: " + propertyKey + ", string: " + valueString
          + ", using default value: " + propertyDefaultValue);
    }

    return propertyDefaultValue;
  }

  @Override
  public synchronized void connect() {
    if (isConnected()) {
      return;
    }

    boolean isStarted = false;
    try {
      _zkclient =
          new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

      _baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
      _configAccessor = new ConfigAccessor(_zkclient);

      _zkclient.subscribeStateChanges(this);
      handleNewSession();

      isStarted = true;
    } catch (Exception e) {
      LOG.error("Exception connect", e);
    } finally {
      if (!isStarted) {
        disconnect();
      }
    }
  }

  @Override
  public synchronized void disconnect() {
    try {
      if (!isConnected()) {
        return;
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("Disconnecting connection: " + this);
      }

      for (final HelixConnectionStateListener listener : _connectionListener) {
        try {

          listener.onDisconnecting();
        } catch (Exception e) {
          LOG.error("Exception in calling disconnect on listener: " + listener, e);
        }
      }
      _zkclient.close();
      _zkclient = null;

      if (LOG.isInfoEnabled()) {
        LOG.info("Disconnected connection: " + this);
      }
    } catch (Exception e) {
      LOG.error("Exception disconnect", e);
    }
  }

  @Override
  public synchronized boolean isConnected() {
    return _zkclient != null;
  }

  @Override
  public HelixParticipant createParticipant(ClusterId clusterId, ParticipantId participantId) {
    return new ZkHelixParticipant(this, clusterId, participantId);
  }

  @Override
  public HelixController createController(ClusterId clusterId, ControllerId controllerId) {
    return new ZkHelixController(this, clusterId, controllerId);
  }

  @Override
  public HelixMultiClusterController createMultiClusterController(ClusterId clusterId,
      ControllerId controllerId) {
    return new ZkHelixMultiClusterController(this, clusterId, controllerId);
  }

  @Override
  public ClusterAccessor createClusterAccessor(ClusterId clusterId) {
    return new ClusterAccessor(clusterId, createDataAccessor(clusterId));
  }

  @Override
  public HelixAdmin createClusterManagementTool() {
    return new ZKHelixAdmin(_zkclient);
  }

  @Override
  public HelixPropertyStore<ZNRecord> createPropertyStore(ClusterId clusterId) {
    PropertyKey key = new PropertyKey.Builder(clusterId.stringify()).propertyStore();
    String fallbackPath = String.format("/%s/%s", clusterId.toString(), "HELIX_PROPERTYSTORE");
    return new AutoFallbackPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkclient),
        key.getPath(), fallbackPath);
  }

  private void checkConnected() {
    if (!isConnected()) {
      throw new IllegalStateException("Connection not connected. Please call #connect()");
    }
  }

  @Override
  public HelixDataAccessor createDataAccessor(ClusterId clusterId) {
    checkConnected();
    return new ZKHelixDataAccessor(clusterId.stringify(), _baseAccessor);
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    return _configAccessor;
  }

  @Override
  public void addControllerListener(HelixRole role, ControllerChangeListener listener,
      ClusterId clusterId) {

    addListener(role, listener, new PropertyKey.Builder(clusterId.stringify()).controller(),
        ChangeType.CONTROLLER, new EventType[] {
            EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
        });
  }

  @Override
  public void addMessageListener(HelixRole role, MessageListener listener, ClusterId clusterId,
      ParticipantId participantId) {

    addListener(role, listener,
        new PropertyKey.Builder(clusterId.stringify()).messages(participantId.stringify()),
        ChangeType.MESSAGE, new EventType[] {
            EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
        });
  }

  @Override
  public void addControllerMessageListener(HelixRole role, MessageListener listener,
      ClusterId clusterId) {

    addListener(role, listener,
        new PropertyKey.Builder(clusterId.stringify()).controllerMessages(),
        ChangeType.MESSAGES_CONTROLLER, new EventType[] {
            EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
        });
  }

  @Override
  public void addIdealStateChangeListener(HelixRole role, IdealStateChangeListener listener,
      ClusterId clusterId) {

    addListener(role, listener, new PropertyKey.Builder(clusterId.stringify()).idealStates(),
        ChangeType.IDEAL_STATE, new EventType[] {
            EventType.NodeDataChanged, EventType.NodeDeleted, EventType.NodeCreated
        });
  }

  @Override
  public void addLiveInstanceChangeListener(HelixRole role, LiveInstanceChangeListener listener,
      ClusterId clusterId) {

    addListener(role, listener, new PropertyKey.Builder(clusterId.stringify()).liveInstances(),
        ChangeType.LIVE_INSTANCE, new EventType[] {
            EventType.NodeDataChanged, EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated
        });
  }

  @Override
  public void addInstanceConfigChangeListener(HelixRole role,
      InstanceConfigChangeListener listener, ClusterId clusterId) {
    addListener(role, listener, new PropertyKey.Builder(clusterId.stringify()).instanceConfigs(),
        ChangeType.INSTANCE_CONFIG, new EventType[] {
          EventType.NodeChildrenChanged
        });
  }

  @Override
  public void addConfigChangeListener(HelixRole role, ScopedConfigChangeListener listener,
      ClusterId clusterId, ConfigScopeProperty scope) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterId.stringify());

    PropertyKey propertyKey = null;
    switch (scope) {
    case CLUSTER:
      propertyKey = keyBuilder.clusterConfigs();
      break;
    case PARTICIPANT:
      propertyKey = keyBuilder.instanceConfigs();
      break;
    case RESOURCE:
      propertyKey = keyBuilder.resourceConfigs();
      break;
    case CONSTRAINT:
      propertyKey = keyBuilder.constraints();
    default:
      break;
    }

    if (propertyKey == null) {
      LOG.error("Failed to add listener: " + listener + ", unrecognized config scope: " + scope);
      return;
    }

    addListener(role, listener, propertyKey, ChangeType.CONFIG, new EventType[] {
      EventType.NodeChildrenChanged
    });
  }

  @Override
  public void addCurrentStateChangeListener(HelixRole role, CurrentStateChangeListener listener,
      ClusterId clusterId, ParticipantId participantId, SessionId sessionId) {

    addListener(role, listener, new PropertyKey.Builder(clusterId.stringify()).currentStates(
        participantId.stringify(), sessionId.stringify()), ChangeType.CURRENT_STATE,
        new EventType[] {
            EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
        });
  }

  @Override
  public void addExternalViewChangeListener(HelixRole role, ExternalViewChangeListener listener,
      ClusterId clusterId) {
    addListener(role, listener, new PropertyKey.Builder(clusterId.stringify()).externalViews(),
        ChangeType.EXTERNAL_VIEW, new EventType[] {
            EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
        });
  }

  @Override
  public boolean removeListener(HelixRole role, Object listener, PropertyKey key) {
    LOG.info("role: " + role + " removing listener: " + listener + " on path: " + key.getPath()
        + " from connection: " + this);
    List<ZkCallbackHandler> toRemove = new ArrayList<ZkCallbackHandler>();
    List<ZkCallbackHandler> handlerList = _handlers.get(role);
    if (handlerList == null) {
      return true;
    }

    synchronized (this) {
      for (ZkCallbackHandler handler : handlerList) {
        // compare property-key path and listener reference
        if (handler.getPath().equals(key.getPath()) && handler.getListener().equals(listener)) {
          toRemove.add(handler);
        }
      }

      handlerList.removeAll(toRemove);
      if (handlerList.isEmpty()) {
        _handlers.remove(role);
      }
    }

    // handler.reset() may modify the handlers list, so do it outside the iteration
    for (ZkCallbackHandler handler : toRemove) {
      handler.reset();
    }

    return true;
  }

  @Override
  public void addConnectionStateListener(HelixConnectionStateListener listener) {
    synchronized (_connectionListener) {
      _connectionListener.add(listener);
    }
  }

  @Override
  public void removeConnectionStateListener(HelixConnectionStateListener listener) {
    synchronized (_connectionListener) {
      _connectionListener.remove(listener);
    }
  }

  @Override
  public synchronized void handleStateChanged(KeeperState state) throws Exception {
    switch (state) {
    case SyncConnected:
      ZkConnection zkConnection = (ZkConnection) _zkclient.getConnection();
      LOG.info("KeeperState: " + state + ", zookeeper:" + zkConnection.getZookeeper());
      break;
    case Disconnected:
      LOG.info("KeeperState:" + state + ", disconnectedSessionId: " + _sessionId);

      /**
       * Track the time stamp that the disconnected happens, then check history and see if
       * we should disconnect the helix-manager
       */
      _disconnectTimeHistory.add(System.currentTimeMillis());
      if (isFlapping()) {
        LOG.error("helix-connection: " + this + ", sessionId: " + _sessionId
            + " is flapping. diconnect it. " + " maxDisconnectThreshold: "
            + _maxDisconnectThreshold + " disconnects in " + _flappingTimeWindowMs + "ms");
        disconnect();
      }
      break;
    case Expired:
      LOG.info("KeeperState:" + state + ", expiredSessionId: " + _sessionId);
      break;
    default:
      break;
    }
  }

  @Override
  public synchronized void handleNewSession() throws Exception {
    waitUntilConnected();

    for (final HelixConnectionStateListener listener : _connectionListener) {
      try {
        listener.onConnected();
      } catch (Exception e) {
        LOG.error("Exception invoking connect on listener: " + listener, e);
      }
    }
  }

  @Override
  public SessionId getSessionId() {
    return _sessionId;
  }

  @Override
  public String getHelixVersion() {
    return _version;
  }

  @Override
  public HelixManagerProperties getHelixProperties() {
    return _properties;
  }

  /**
   * wait until we get a non-zero session-id. note that we might lose zkconnection
   * right after we read session-id. but it's ok to get stale session-id and we will have
   * another handle-new-session callback to correct this.
   */
  private void waitUntilConnected() {
    boolean isConnected;
    do {
      isConnected =
          _zkclient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!isConnected) {
        LOG.error("fail to connect zkserver: " + _zkAddr + " in "
            + ZkClient.DEFAULT_CONNECTION_TIMEOUT + "ms. expiredSessionId: " + _sessionId);
        continue;
      }

      ZkConnection zkConnection = ((ZkConnection) _zkclient.getConnection());
      _sessionId = SessionId.from(Long.toHexString(zkConnection.getZookeeper().getSessionId()));

      /**
       * at the time we read session-id, zkconnection might be lost again
       * wait until we get a non-zero session-id
       */
    } while (!isConnected || "0".equals(_sessionId));

    LOG.info("Handling new session, session id: " + _sessionId + ", zkconnection: "
        + ((ZkConnection) _zkclient.getConnection()).getZookeeper());
  }

  @Override
  public int getSessionTimeout() {
    return _sessionTimeout;
  }

  @Override
  public ClusterMessagingService createMessagingService(HelixRole role) {
    HelixManager manager = new ZKHelixManager(role);
    return new DefaultMessagingService(manager);
  }

  void addListener(HelixRole role, Object listener, PropertyKey propertyKey, ChangeType changeType,
      EventType[] eventType) {
    // checkConnected();
    PropertyType type = propertyKey.getType();

    synchronized (this) {
      if (!_handlers.containsKey(role)) {
        _handlers.put(role, new CopyOnWriteArrayList<ZkCallbackHandler>());
      }
      List<ZkCallbackHandler> handlerList = _handlers.get(role);

      for (ZkCallbackHandler handler : handlerList) {
        // compare property-key path and listener reference
        if (handler.getPath().equals(propertyKey.getPath())
            && handler.getListener().equals(listener)) {
          LOG.info("role: " + role + ", listener: " + listener + " on path: "
              + propertyKey.getPath() + " already exists. skip add");

          return;
        }
      }

      ZkCallbackHandler newHandler =
          new ZkCallbackHandler(role, _zkclient, propertyKey, listener, eventType, changeType);

      handlerList.add(newHandler);
      LOG.info("role: " + role + " added listener: " + listener + " for type: " + type
          + " to path: " + newHandler.getPath());
    }
  }

  void initHandlers(HelixRole role) {
    synchronized (this) {
      List<ZkCallbackHandler> handlerList = _handlers.get(role);

      if (handlerList != null) {
        for (ZkCallbackHandler handler : handlerList) {
          handler.init();
          LOG.info("role: " + role + ", init handler: " + handler.getPath() + ", "
              + handler.getListener());
        }
      }
    }
  }

  void resetHandlers(HelixRole role) {
    synchronized (this) {
      List<ZkCallbackHandler> handlerList = _handlers.get(role);

      if (handlerList != null) {
        for (ZkCallbackHandler handler : handlerList) {
          handler.reset();
          LOG.info("role: " + role + ", reset handler: " + handler.getPath() + ", "
              + handler.getListener());
        }
      }
    }
  }

  /**
   * If zk state has changed into DISCONNECTED for _maxDisconnectThreshold times during
   * _timeWindowLengthMs time window, it's flapping and we tear down the zk-connection
   */
  private boolean isFlapping() {
    if (_disconnectTimeHistory.size() == 0) {
      return false;
    }
    long mostRecentTimestamp = _disconnectTimeHistory.get(_disconnectTimeHistory.size() - 1);

    // Remove disconnect history timestamp that are older than _flappingTimeWindowMs ago
    while ((_disconnectTimeHistory.get(0) + _flappingTimeWindowMs) < mostRecentTimestamp) {
      _disconnectTimeHistory.remove(0);
    }
    return _disconnectTimeHistory.size() > _maxDisconnectThreshold;
  }
}
