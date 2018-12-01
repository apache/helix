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

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.*;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import org.apache.helix.healthcheck.ParticipantHealthReportTask;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.ZKPathDataDumpTask;
import org.apache.helix.monitoring.mbeans.HelixCallbackMonitor;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.AutoFallbackPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.util.HelixUtil;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class ZKHelixManager implements HelixManager, IZkStateListener {
  private static Logger LOG = LoggerFactory.getLogger(ZKHelixManager.class);

  public static final String ALLOW_PARTICIPANT_AUTO_JOIN = "allowParticipantAutoJoin";
  private static final int FLAPPING_TIME_WINDOW = 300000; // Default to 300 sec
  public static final int DEFAULT_MAX_DISCONNECT_THRESHOLD = 600; // Default to be a large number
  private static final int DEFAULT_WAIT_CONNECTED_TIMEOUT = 10 * 1000;  // wait until connected for up to 10 seconds.

  protected final String _zkAddress;
  private final String _clusterName;
  private final String _instanceName;
  private final InstanceType _instanceType;
  private final int _waitForConnectedTimeout; // wait time for testing connect
  private final int _sessionTimeout; // client side session timeout, will be overridden by server timeout. Disconnect after timeout
  private final int _connectionInitTimeout; // client timeout to init connect
  private final List<PreConnectCallback> _preConnectCallbacks;
  protected final List<CallbackHandler> _handlers;
  private final HelixManagerProperties _properties;
  private final HelixManagerStateListener _stateListener;

  /**
   * helix version#
   */
  private final String _version;
  private int _reportLatency;

  protected HelixZkClient _zkclient = null;
  private final DefaultMessagingService _messagingService;
  private Map<ChangeType, HelixCallbackMonitor> _callbackMonitors;

  private BaseDataAccessor<ZNRecord> _baseDataAccessor;
  private ZKHelixDataAccessor _dataAccessor;
  private final Builder _keyBuilder;
  private ConfigAccessor _configAccessor;
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  protected LiveInstanceInfoProvider _liveInstanceInfoProvider = null;

  private volatile String _sessionId;

  /**
   * Keep track of timestamps that zk State has become Disconnected If in a _timeWindowLengthMs
   * window zk State has become Disconnected for more than_maxDisconnectThreshold times disconnect
   * the zkHelixManager
   */
  private final List<Long> _disconnectTimeHistory = new ArrayList<Long>();
  private final int _flappingTimeWindowMs;
  private final int _maxDisconnectThreshold;

  /**
   * participant fields
   */
  private final StateMachineEngine _stateMachineEngine;
  private final List<HelixTimerTask> _timerTasks = new ArrayList<>();

  private final ParticipantHealthReportCollectorImpl _participantHealthInfoCollector;
  private Long _sessionStartTime;
  private ParticipantManager _participantManager;

  /**
   * controller fields
   */
  private GenericHelixController _controller;
  private CallbackHandler _leaderElectionHandler = null;
  protected final List<HelixTimerTask> _controllerTimerTasks = new ArrayList<>();

  /**
   * status dump timer-task
   */
  static class StatusDumpTask extends HelixTimerTask {
    Timer _timer = null;
    final HelixManager helixController;

    public StatusDumpTask(HelixManager helixController) {
      this.helixController = helixController;
    }

    @Override
    public void start() {
      long initialDelay = 0;
      long period = 15 * 60 * 1000;
      long timeThresholdNoChangeForStatusUpdates = 15 * 60 * 1000; // 15 minutes
      long timeThresholdNoChangeForErrors = 24 * 60 * 60 * 1000; // 1 day
      int maximumNumberOfLeafNodesAllowed = 100;

      if (_timer == null) {
        LOG.info("Start StatusDumpTask");
        _timer = new Timer("StatusDumpTimerTask", true);
        _timer.scheduleAtFixedRate(
            new ZKPathDataDumpTask(helixController, timeThresholdNoChangeForStatusUpdates,
                timeThresholdNoChangeForErrors, maximumNumberOfLeafNodesAllowed), initialDelay,
            period);
      }
    }

    @Override
    public void stop() {
      if (_timer != null) {
        LOG.info("Stop StatusDumpTask");
        _timer.cancel();
        _timer = null;
      }
    }
  }

  public ZKHelixManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddress) {
    this(clusterName, instanceName, instanceType, zkAddress, null);
  }

  public ZKHelixManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddress, HelixManagerStateListener stateListener) {

    LOG.info(
        "Create a zk-based cluster manager. zkSvr: " + zkAddress + ", clusterName: " + clusterName + ", instanceName: " + instanceName + ", type: " + instanceType);

    _zkAddress = zkAddress;
    _clusterName = clusterName;
    _instanceType = instanceType;

    if (instanceName == null) {
      try {
        instanceName =
            InetAddress.getLocalHost().getCanonicalHostName() + "-" + instanceType.toString();
      } catch (UnknownHostException e) {
        // can ignore it
        LOG.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
        instanceName = "UNKNOWN";
      }
    }

    _instanceName = instanceName;
    _preConnectCallbacks = new ArrayList<>();
    _handlers = new ArrayList<>();
    _properties = new HelixManagerProperties(SystemPropertyKeys.CLUSTER_MANAGER_VERSION);
    _version = _properties.getVersion();

    _keyBuilder = new Builder(clusterName);
    _messagingService = new DefaultMessagingService(this);
    try {
      _callbackMonitors = new HashMap<>();
      for (ChangeType changeType : ChangeType.values()) {
        HelixCallbackMonitor callbackMonitor =
            new HelixCallbackMonitor(instanceType, clusterName, instanceName, changeType);
        callbackMonitor.register();
        _callbackMonitors.put(changeType, callbackMonitor);
      }
    } catch (JMException e) {
      LOG.error("Error in creating callback monitor.", e);
    }

    _stateListener = stateListener;

    /**
     * use system property if available
     */
    _flappingTimeWindowMs = HelixUtil.getSystemPropertyAsInt(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        ZKHelixManager.FLAPPING_TIME_WINDOW);

    _maxDisconnectThreshold = HelixUtil
        .getSystemPropertyAsInt(SystemPropertyKeys.MAX_DISCONNECT_THRESHOLD,
            ZKHelixManager.DEFAULT_MAX_DISCONNECT_THRESHOLD);

    _sessionTimeout = HelixUtil.getSystemPropertyAsInt(SystemPropertyKeys.ZK_SESSION_TIMEOUT,
        HelixZkClient.DEFAULT_SESSION_TIMEOUT);

    _connectionInitTimeout = HelixUtil
        .getSystemPropertyAsInt(SystemPropertyKeys.ZK_CONNECTION_TIMEOUT,
            HelixZkClient.DEFAULT_CONNECTION_TIMEOUT);

    _waitForConnectedTimeout = HelixUtil
        .getSystemPropertyAsInt(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT,
            DEFAULT_WAIT_CONNECTED_TIMEOUT);

    _reportLatency = HelixUtil
        .getSystemPropertyAsInt(SystemPropertyKeys.PARTICIPANT_HEALTH_REPORT_LATENCY,
            ParticipantHealthReportTask.DEFAULT_REPORT_LATENCY);

    /**
     * instance type specific init
     */
    switch (instanceType) {
    case PARTICIPANT:
      _stateMachineEngine = new HelixStateMachineEngine(this);
      _participantHealthInfoCollector =
          new ParticipantHealthReportCollectorImpl(this, _instanceName);
      _timerTasks
          .add(new ParticipantHealthReportTask(_participantHealthInfoCollector, _reportLatency));
      break;
    case CONTROLLER:
      _stateMachineEngine = null;
      _participantHealthInfoCollector = null;
      _controllerTimerTasks.add(new StatusDumpTask(this));

      break;
    case CONTROLLER_PARTICIPANT:
      _stateMachineEngine = new HelixStateMachineEngine(this);
      _participantHealthInfoCollector =
          new ParticipantHealthReportCollectorImpl(this, _instanceName);

      _timerTasks
          .add(new ParticipantHealthReportTask(_participantHealthInfoCollector, _reportLatency));
      _controllerTimerTasks.add(new StatusDumpTask(this));
      break;
    case ADMINISTRATOR:
    case SPECTATOR:
      _stateMachineEngine = null;
      _participantHealthInfoCollector = null;
      break;
    default:
      throw new IllegalArgumentException("unrecognized type: " + instanceType);
    }
  }

  @Override public boolean removeListener(PropertyKey key, Object listener) {
    LOG.info("Removing listener: " + listener + " on path: " + key.getPath() + " from cluster: "
        + _clusterName + " by instance: " + _instanceName);

    synchronized (this) {
      List<CallbackHandler> toRemove = new ArrayList<>();
      for (CallbackHandler handler : _handlers) {
        // compare property-key path and listener reference
        if (handler.getPath().equals(key.getPath()) && handler.getListener().equals(listener)) {
          toRemove.add(handler);
        }
      }

      _handlers.removeAll(toRemove);

      // handler.reset() may modify the handlers list, so do it outside the iteration
      for (CallbackHandler handler : toRemove) {
        handler.reset(true);
      }
    }

    return true;
  }

  void checkConnected() {
    checkConnected(-1);
  }

  /**
   * Check if HelixManager is connected, if it is not connected,
   * wait for the specified timeout and check again before return.
   *
   * @param timeout
   */
  void checkConnected(long timeout) {
    if (_zkclient == null || _zkclient.isClosed()) {
      throw new HelixException(
          "HelixManager (ZkClient) is not connected. Call HelixManager#connect()");
    }

    boolean isConnected = isConnected();
    if (!isConnected && timeout > 0) {
      LOG.warn(
          "zkClient to " + _zkAddress + " is not connected, wait for " + _waitForConnectedTimeout
              + "ms.");
      isConnected = _zkclient.waitUntilConnected(_waitForConnectedTimeout, TimeUnit.MILLISECONDS);
    }

    if (!isConnected) {
      LOG.error("zkClient is not connected after waiting " + timeout + "ms."
          + ", clusterName: " + _clusterName + ", zkAddress: " + _zkAddress);
      throw new HelixException(
          "HelixManager is not connected within retry timeout for cluster " + _clusterName);
    }
  }

  void addListener(Object listener, PropertyKey propertyKey, ChangeType changeType,
      EventType[] eventType) {
    checkConnected(_waitForConnectedTimeout);

    PropertyType type = propertyKey.getType();

    synchronized (this) {
      for (CallbackHandler handler : _handlers) {
        // compare property-key path and listener reference
        if (handler.getPath().equals(propertyKey.getPath())
            && handler.getListener().equals(listener)) {
          LOG.info("Listener: " + listener + " on path: " + propertyKey.getPath()
              + " already exists. skip add");

          return;
        }
      }

      CallbackHandler newHandler =
          new CallbackHandler(this, _zkclient, propertyKey, listener, eventType, changeType,
              _callbackMonitors.get(changeType));

      _handlers.add(newHandler);
      LOG.info("Added listener: " + listener + " for type: " + type + " to path: "
          + newHandler.getPath());
    }
  }

  @Override
  public void addIdealStateChangeListener(final IdealStateChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).idealStates(), ChangeType.IDEAL_STATE,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Deprecated
  @Override
  public void addIdealStateChangeListener(final org.apache.helix.IdealStateChangeListener listener)
      throws Exception {
    addListener(listener, new Builder(_clusterName).idealStates(), ChangeType.IDEAL_STATE,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).liveInstances(), ChangeType.LIVE_INSTANCE,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Deprecated
  @Override
  public void addLiveInstanceChangeListener(org.apache.helix.LiveInstanceChangeListener listener)
      throws Exception {
    addListener(listener, new Builder(_clusterName).liveInstances(), ChangeType.LIVE_INSTANCE,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).instanceConfigs(), ChangeType.INSTANCE_CONFIG,
        new EventType[] { EventType.NodeChildrenChanged
        });
  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener)
      throws Exception {
    addListener(listener, new Builder(_clusterName).instanceConfigs(), ChangeType.INSTANCE_CONFIG,
        new EventType[] { EventType.NodeChildrenChanged
        });
  }

  @Deprecated
  @Override
  public void addInstanceConfigChangeListener(org.apache.helix.InstanceConfigChangeListener listener)
      throws Exception {
    addListener(listener, new Builder(_clusterName).instanceConfigs(), ChangeType.INSTANCE_CONFIG,
        new EventType[] { EventType.NodeChildrenChanged
        });
  }

  @Override
  public void addResourceConfigChangeListener(ResourceConfigChangeListener listener) throws Exception{
    addListener(listener, new Builder(_clusterName).resourceConfigs(), ChangeType.RESOURCE_CONFIG,
        new EventType[] { EventType.NodeChildrenChanged
        });
  }

  @Override
  public void addClusterfigChangeListener(ClusterConfigChangeListener listener) throws Exception{
    addListener(listener, new Builder(_clusterName).clusterConfig(), ChangeType.CLUSTER_CONFIG,
        new EventType[] { EventType.NodeDataChanged
        });
  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception {
    Builder keyBuilder = new Builder(_clusterName);

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
    default:
      break;
    }

    if (propertyKey != null) {
      addListener(listener, propertyKey, ChangeType.CONFIG, new EventType[] {
        EventType.NodeChildrenChanged
      });
    } else {
      LOG.error("Can't add listener to config scope: " + scope);
    }
  }


  @Deprecated
  @Override
  public void addConfigChangeListener(org.apache.helix.ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception {
    addConfigChangeListener((ScopedConfigChangeListener) listener, scope);
  }

  // TODO: Decide if do we still need this since we are exposing
  // ClusterMessagingService
  @Override
  public void addMessageListener(MessageListener listener, String instanceName) {
    addListener(listener, new Builder(_clusterName).messages(instanceName), ChangeType.MESSAGE,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Deprecated
  @Override
  public void addMessageListener(org.apache.helix.MessageListener listener, String instanceName) {
    addListener(listener, new Builder(_clusterName).messages(instanceName), ChangeType.MESSAGE,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public void addControllerMessageListener(MessageListener listener) {
    addListener(listener, new Builder(_clusterName).controllerMessages(),
        ChangeType.MESSAGES_CONTROLLER, new EventType[] { EventType.NodeChildrenChanged });
  }

  @Deprecated
  @Override
  public void addControllerMessageListener(org.apache.helix.MessageListener listener) {
    addListener(listener, new Builder(_clusterName).controllerMessages(),
        ChangeType.MESSAGES_CONTROLLER, new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {
    addListener(listener, new Builder(_clusterName).currentStates(instanceName, sessionId),
        ChangeType.CURRENT_STATE, new EventType[] { EventType.NodeChildrenChanged
        });
  }

  @Deprecated
  @Override
  public void addCurrentStateChangeListener(org.apache.helix.CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {
    addListener(listener, new Builder(_clusterName).currentStates(instanceName, sessionId),
        ChangeType.CURRENT_STATE, new EventType[] { EventType.NodeChildrenChanged
        });
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).externalViews(), ChangeType.EXTERNAL_VIEW,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public void addTargetExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).externalViews(), ChangeType.TARGET_EXTERNAL_VIEW,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Deprecated
  @Override
  public void addExternalViewChangeListener(org.apache.helix.ExternalViewChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).externalViews(), ChangeType.EXTERNAL_VIEW,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener) {
    addListener(listener, new Builder(_clusterName).controller(), ChangeType.CONTROLLER,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Deprecated
  @Override
  public void addControllerListener(org.apache.helix.ControllerChangeListener listener) {
    addListener(listener, new Builder(_clusterName).controller(), ChangeType.CONTROLLER,
        new EventType[] { EventType.NodeChildrenChanged });
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    checkConnected(_waitForConnectedTimeout);
    return _dataAccessor;
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    checkConnected(_waitForConnectedTimeout);
    return _configAccessor;
  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Returns a string that can be used to connect to metadata store for this HelixManager instance
   * i.e. for ZkHelixManager, this will have format "{zookeeper-address}:{port}"
   * @return a string used to connect to metadata store
   */
  @Override
  public String getMetadataStoreConnectionString() {
    return _zkAddress;
  }

  @Override
  public String getInstanceName() {
    return _instanceName;
  }

  BaseDataAccessor<ZNRecord> createBaseDataAccessor() {
    ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(_zkclient);
    return baseDataAccessor;
  }

  /**
   * Add Helix built-in state model definitions if not exist
   */
  private void addBuiltInStateModelDefinitions() {
    for (BuiltInStateModelDefinitions def : BuiltInStateModelDefinitions.values()) {
      // creation succeeds only if not exist
      _dataAccessor.createStateModelDef(def.getStateModelDefinition());
    }
  }

  void createClient() throws Exception {
    PathBasedZkSerializer zkSerializer =
        ChainedPathZkSerializer.builder(new ZNRecordSerializer()).build();

    HelixZkClient.ZkConnectionConfig connectionConfig = new HelixZkClient.ZkConnectionConfig(_zkAddress);
    connectionConfig.setSessionTimeout(_sessionTimeout);
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig
        .setZkSerializer(zkSerializer)
        .setConnectInitTimeout(_connectionInitTimeout)
        .setMonitorType(_instanceType.name())
        .setMonitorKey(_clusterName)
        .setMonitorInstanceName(_instanceName)
        .setMonitorRootPathOnly(!_instanceType.equals(InstanceType.CONTROLLER) && !_instanceType
            .equals(InstanceType.CONTROLLER_PARTICIPANT));

    HelixZkClient newClient;
    switch (_instanceType) {
    case ADMINISTRATOR:
      newClient = SharedZkClientFactory.getInstance().buildZkClient(connectionConfig, clientConfig);
      break;
    default:
      newClient = DedicatedZkClientFactory
          .getInstance().buildZkClient(connectionConfig, clientConfig);
      break;
    }

    synchronized (this) {
      if (_zkclient != null) {
        _zkclient.close();
      }
      _zkclient = newClient;

      _baseDataAccessor = createBaseDataAccessor();

      _dataAccessor = new ZKHelixDataAccessor(_clusterName, _instanceType, _baseDataAccessor);
      _configAccessor = new ConfigAccessor(_zkclient);

      if (_instanceType == InstanceType.CONTROLLER
          || _instanceType == InstanceType.CONTROLLER_PARTICIPANT) {
        addBuiltInStateModelDefinitions();
      }
    }

    // subscribe to state change
    _zkclient.subscribeStateChanges(this);
    int retryCount = 0;
    while (retryCount < 3) {
      try {
        _zkclient.waitUntilConnected(_connectionInitTimeout, TimeUnit.MILLISECONDS);
        handleStateChanged(KeeperState.SyncConnected);
        handleNewSession();
        break;
      } catch (HelixException e) {
        LOG.error("fail to createClient.", e);
        throw e;
      } catch (Exception e) {
        retryCount++;
        LOG.error("fail to createClient. retry " + retryCount, e);
        if (retryCount == 3) {
          throw e;
        }
      }
    }
  }

  @Override
  public void connect() throws Exception {
    LOG.info("ClusterManager.connect()");
    if (isConnected()) {
      LOG.warn("Cluster manager: " + _instanceName + " for cluster: " + _clusterName
          + " already connected. skip connect");
      return;
    }

    switch (_instanceType) {
    case CONTROLLER:
    case CONTROLLER_PARTICIPANT:
      if (_controller == null) {
        _controller = new GenericHelixController(_clusterName);
        _messagingService.getExecutor().setController(_controller);
      }
      break;
    default:
      break;
    }

    try {
      createClient();
      _messagingService.onConnected();
    } catch (Exception e) {
      LOG.error("fail to connect " + _instanceName, e);
      try {
        disconnect();
      } catch (Exception ex) {
        // if zk connection fails to be created, disconnect may throw exception about reporting disconnect to ZK.
      }
      throw e;
    }
  }

  @Override
  public void disconnect() {
    if (_zkclient == null || _zkclient.isClosed()) {
      LOG.info("instanceName: " + _instanceName + " already disconnected");
      return;
    }

    LOG.info("disconnect " + _instanceName + "(" + _instanceType + ") from " + _clusterName);

    try {
      /**
       * stop all timer tasks
       */
      stopTimerTasks();

      /**
       * shutdown thread pool first to avoid reset() being invoked in the middle of state
       * transition
       */
      _messagingService.getExecutor().shutdown();

      // TODO reset user defined handlers only
      // TODO Fix the issue that when connection disconnected, reset handlers will be blocked. -- JJ
      // This is because reset logic contains ZK operations.
      resetHandlers(true);

      if (_leaderElectionHandler != null) {
        _leaderElectionHandler.reset(true);
      }

    } finally {
      GenericHelixController controller = _controller;
      if (controller != null) {
        try {
          controller.shutdown();
        } catch (InterruptedException e) {
          LOG.info("Interrupted shutting down GenericHelixController", e);
        }
      }

      ParticipantManager participantManager = _participantManager;
      if (participantManager != null) {
        participantManager.disconnect();
      }

      for (HelixCallbackMonitor callbackMonitor : _callbackMonitors.values()) {
        callbackMonitor.unregister();
      }

      _helixPropertyStore = null;

      synchronized (this) {
        if (_controller != null) {
          _controller = null;
          _leaderElectionHandler = null;
        }

        if (_participantManager != null) {
          _participantManager = null;
        }

        if (_zkclient != null) {
          _zkclient.close();
        }
      }
      _sessionStartTime = null;
      LOG.info("Cluster manager: " + _instanceName + " disconnected");
    }
  }

  @Override
  public String getSessionId() {
    checkConnected(_waitForConnectedTimeout);
    return _sessionId;
  }

  @Override
  public boolean isConnected() {
    if (_zkclient == null || _zkclient.isClosed()) {
      return false;
    }
    // Don't check ZkConnection state, which is different from ZkClient's watcher state.
    // ZkConnection state is the internal state of the connection, which can be different from the
    // ZkClient state due to internal thread/retry logic.
    try {
      return _zkclient.waitUntilConnected(0, TimeUnit.MILLISECONDS);
    } catch (ZkInterruptedException ex) {
      return false;
    }
  }

  @Override
  public long getLastNotificationTime() {
    return 0;
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    LOG.info("Adding preconnect callback: " + callback);
    _preConnectCallbacks.add(callback);
  }

  @Override
  public boolean isLeader() {
    String warnLogPrefix = String
        .format("Instance %s is not leader of cluster %s due to", _instanceName, _clusterName);
    if (_instanceType != InstanceType.CONTROLLER
        && _instanceType != InstanceType.CONTROLLER_PARTICIPANT) {
      LOG.warn(String
          .format("%s instance type %s does not match to CONTROLLER/CONTROLLER_PARTICIPANT",
              warnLogPrefix, _instanceType.name()));
      return false;
    }

    if (!isConnected()) {
      LOG.warn(String.format("%s HelixManager is not connected", warnLogPrefix));
      return false;
    }

    try {
      LiveInstance leader = _dataAccessor.getProperty(_keyBuilder.controllerLeader());
      if (leader != null) {
        String leaderName = leader.getInstanceName();
        String sessionId = Long.toHexString(leader.getRecord().getEphemeralOwner());
        if (leaderName != null && leaderName.equals(_instanceName) && sessionId
            .equals(_sessionId)) {
          return true;
        }
        LOG.warn(String
            .format("%s current session %s does not match leader session %s", warnLogPrefix,
                _sessionId, sessionId));
      }
      LOG.warn(String.format("%s leader ZNode is null", warnLogPrefix));

    } catch (Exception e) {
      // log
      LOG.warn(String.format("%s exception happen when session check", warnLogPrefix), e);
    }
    return false;
  }

  @Override
  public synchronized ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    checkConnected(_waitForConnectedTimeout);

    if (_helixPropertyStore == null) {
      String path = PropertyPathBuilder.propertyStore(_clusterName);
      String fallbackPath = String.format("/%s/%s", _clusterName, "HELIX_PROPERTYSTORE");
      _helixPropertyStore =
          new AutoFallbackPropertyStore<>(new ZkBaseDataAccessor<ZNRecord>(_zkclient),
              path, fallbackPath);
    }

    return _helixPropertyStore;
  }

  @Override
  public synchronized HelixAdmin getClusterManagmentTool() {
    checkConnected(_waitForConnectedTimeout);
    if (_zkclient != null && !_zkclient.isClosed()) {
      return new ZKHelixAdmin(_zkclient);
    }

    LOG.error("Couldn't get ZKClusterManagementTool because zkclient is null");
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    // The caller can register message handler factories on messaging service before the
    // helix manager is connected. Thus we do not do connected check here.
    return _messagingService;
  }

  @Override
  public InstanceType getInstanceType() {
    return _instanceType;
  }

  @Override
  public String getVersion() {
    return _version;
  }

  @Override
  public HelixManagerProperties getProperties() {
    return _properties;
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    return _stateMachineEngine;
  }

  // TODO: rename this and not expose this function as part of interface
  @Override
  public void startTimerTasks() {
    for (HelixTimerTask task : _timerTasks) {
      task.start();
    }
  }

  @Override
  public void stopTimerTasks() {
    for (HelixTimerTask task : _timerTasks) {
      task.stop();
    }
  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    _liveInstanceInfoProvider = liveInstanceInfoProvider;
  }

  /**
   * wait until we get a non-zero session-id. note that we might lose zkconnection
   * right after we read session-id. but it's ok to get stale session-id and we will have
   * another handle-new-session callback to correct this.
   */
  void waitUntilConnected() {
    boolean isConnected;
    do {
      isConnected =
          _zkclient.waitUntilConnected(HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!isConnected) {
        LOG.error("fail to connect zkserver: " + _zkAddress + " in "
            + HelixZkClient.DEFAULT_CONNECTION_TIMEOUT + "ms. expiredSessionId: " + _sessionId
            + ", clusterName: " + _clusterName);
        continue;
      }

      _sessionId = Long.toHexString(_zkclient.getSessionId());

      /**
       * at the time we read session-id, zkconnection might be lost again
       * wait until we get a non-zero session-id
       */
    } while (!isConnected || "0".equals(_sessionId));

    LOG.info("Handling new session, session id: " + _sessionId + ", instance: " + _instanceName
        + ", instanceTye: " + _instanceType + ", cluster: " + _clusterName);
  }

  void initHandlers(List<CallbackHandler> handlers) {
    synchronized (this) {
      if (handlers != null) {
        for (CallbackHandler handler : handlers) {
          handler.init();
          LOG.info("init handler: " + handler.getPath() + ", " + handler.getListener());
        }
      }
    }
  }

  void resetHandlers(boolean isShutdown) {
    synchronized (this) {
      if (_handlers != null) {
        // get a copy of the list and iterate over the copy list
        // in case handler.reset() modify the original handler list
        List<CallbackHandler> tmpHandlers = new ArrayList<>();
        tmpHandlers.addAll(_handlers);

        for (CallbackHandler handler : tmpHandlers) {
          handler.reset(isShutdown);
          LOG.info("reset handler: " + handler.getPath() + ", " + handler.getListener());
        }
      }
    }
  }

  /**
   * If zk state has changed into Disconnected for maxDisconnectThreshold times during previous
   * timeWindowLengthMs Ms
   * time window, we think that there are something wrong going on and disconnect the zkHelixManager
   * from zk.
   */
  boolean isFlapping() {
    if (_disconnectTimeHistory.size() == 0) {
      return false;
    }
    long mostRecentTimestamp = _disconnectTimeHistory.get(_disconnectTimeHistory.size() - 1);

    // Remove disconnect history timestamp that are older than flappingTimeWindowMs ago
    while ((_disconnectTimeHistory.get(0) + _flappingTimeWindowMs) < mostRecentTimestamp) {
      _disconnectTimeHistory.remove(0);
    }
    return _disconnectTimeHistory.size() > _maxDisconnectThreshold;
  }

  @Override
  public void handleStateChanged(KeeperState state) {
    switch (state) {
    case SyncConnected:
      LOG.info("KeeperState: " + state + ", instance: " + _instanceName + ", type: " + _instanceType);
      break;
    case Disconnected:
      /**
       * Track the time stamp that the disconnected happens, then check history and see if
       * we should disconnect the helix-manager
       */
      _disconnectTimeHistory.add(System.currentTimeMillis());
      if (isFlapping()) {
        String errorMsg = "instanceName: " + _instanceName + " is flapping. disconnect it. "
            + " maxDisconnectThreshold: " + _maxDisconnectThreshold + " disconnects in "
            + _flappingTimeWindowMs + "ms.";
        LOG.error(errorMsg);

        // Only disable the instance when it's instance type is PARTICIPANT
        if (_instanceType.equals(InstanceType.PARTICIPANT)) {
          LOG.warn("instanceName: " + _instanceName
              + " is flapping. Since it is a participant, disable it.");
          try {
            getClusterManagmentTool().enableInstance(_clusterName, _instanceName, false);
          } catch (Exception e) {
            LOG.error("Failed to disable participant before disconnecting participant.", e);
          }
        }

        try {
          // TODO Call disconnect in another thread.
          // handleStateChanged is triggered in ZkClient eventThread. The disconnect logic will
          // interrupt this thread. This issue prevents the ZkClient.close() from complete. So the
          // client is left in a strange state.
          disconnect();
        } catch (Exception ex) {
          LOG.error("Disconnect HelixManager is not completely done.", ex);
        }

        if (_stateListener != null) {
          try {
            _stateListener.onDisconnected(this, new HelixException(errorMsg));
          } catch (Exception e) {
            LOG.warn("stateListener.onDisconnected callback fails", e);
          }
        }
        break;
      }
    // if not flapping, share the continuous logic with Expired case
    case Expired:
      LOG.warn("KeeperState:" + state + ", SessionId: " + _sessionId + ", instance: "
          + _instanceName + ", type: " + _instanceType);
      break;
    default:
      LOG.info("KeeperState:" + state + ", currentSessionId: " + _sessionId + ", instance: "
          + _instanceName + ", type: " + _instanceType);
      break;
    }
  }

  @Override
  public void handleNewSession() throws Exception {
    LOG.info(
        "Handle new session, instance: " + _instanceName + ", type: " + _instanceType);
    waitUntilConnected();

    /**
     * stop all timer tasks, reset all handlers, make sure cleanup completed for previous session
     * disconnect if fail to cleanup
     */
    stopTimerTasks();
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.reset(false);
    }
    resetHandlers(false);

    /**
     * clean up write-through cache
     */
    _baseDataAccessor.reset();

    /**
     * from here on, we are dealing with new session
     */
    if (!ZKUtil.isClusterSetup(_clusterName, _zkclient)) {
      throw new HelixException("Cluster structure is not set up for cluster: " + _clusterName);
    }

    _sessionStartTime = System.currentTimeMillis();

    switch (_instanceType) {
    case PARTICIPANT:
      handleNewSessionAsParticipant();
      break;
    case CONTROLLER:
      handleNewSessionAsController();
      break;
    case CONTROLLER_PARTICIPANT:
      handleNewSessionAsParticipant();
      handleNewSessionAsController();
      break;
    case ADMINISTRATOR:
    case SPECTATOR:
    default:
      break;
    }

    startTimerTasks();

    /**
     * init handlers
     * ok to init message handler and data-accessor twice
     * the second init will be skipped (see CallbackHandler)
     */
    initHandlers(_handlers);

    if (_stateListener != null) {
      try {
        _stateListener.onConnected(this);
      } catch (Exception e) {
        LOG.warn("stateListener.onConnected callback fails", e);
      }
    }
  }

  void handleNewSessionAsParticipant() throws Exception {
    if (_participantManager != null) {
      _participantManager.reset();
    }
    _participantManager =
        new ParticipantManager(this, _zkclient, _sessionTimeout, _liveInstanceInfoProvider,
            _preConnectCallbacks);
    _participantManager.handleNewSession();
  }

  void handleNewSessionAsController() {
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.init();
    } else {
      _leaderElectionHandler =
          new CallbackHandler(this, _zkclient, _keyBuilder.controller(),
              new DistributedLeaderElection(this, _controller, _controllerTimerTasks),
              new EventType[] {
                  EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated
              }, ChangeType.CONTROLLER, _callbackMonitors.get(ChangeType.CONTROLLER));
    }
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector() {
    return _participantHealthInfoCollector;
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) throws Exception {
    LOG.warn("Handling Session Establishment Error. Disconnect Helix Manager.", error);
    disconnect();

    if (_stateListener != null) {
      _stateListener.onDisconnected(this, error);
    }
  }

  @Override
  public Long getSessionStartTime() {
    return _sessionStartTime;
  }
}
