/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.manager.zk;

import static com.linkedin.helix.HelixConstants.ChangeType.CONFIG;
import static com.linkedin.helix.HelixConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.helix.HelixConstants.ChangeType.HEALTH;
import static com.linkedin.helix.HelixConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.helix.HelixConstants.ChangeType.MESSAGE;
import static com.linkedin.helix.HelixConstants.ChangeType.MESSAGES_CONTROLLER;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixTimerTask;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.PreConnectCallback;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.restlet.ZKPropertyTransferServer;
import com.linkedin.helix.healthcheck.HealthStatsAggregationTask;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollectorImpl;
import com.linkedin.helix.messaging.DefaultMessagingService;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.monitoring.ZKPathDataDumpTask;
import com.linkedin.helix.participant.DistClusterControllerElection;
import com.linkedin.helix.participant.HelixStateMachineEngine;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.ZNRecordJsonSerializer;
import com.linkedin.helix.store.zk.ZKPropertyStore;
import com.linkedin.helix.store.zk.ZkHelixPropertyStore;
import com.linkedin.helix.tools.PropertiesReader;

public class ZKHelixManager implements HelixManager
{
  private static Logger                        logger                  =
                                                                           Logger.getLogger(ZKHelixManager.class);
  private static final int                     RETRY_LIMIT             = 3;
  private static final int                     CONNECTIONTIMEOUT       = 60 * 1000;
  private final String                         _clusterName;
  private final String                         _instanceName;
  private final String                         _zkConnectString;
  private static final int                     DEFAULT_SESSION_TIMEOUT = 30 * 1000;
  private ZKDataAccessor                       _accessor;
  private ZKHelixDataAccessor                  _helixAccessor;
  private ConfigAccessor                       _configAccessor;
  protected ZkClient                           _zkClient;
  private final List<CallbackHandler>          _handlers;
  private final ZkStateChangeListener          _zkStateChangeListener;
  private final InstanceType                   _instanceType;
  volatile String                              _sessionId;
  private Timer                                _timer;
  private CallbackHandler                      _leaderElectionHandler;
  private ParticipantHealthReportCollectorImpl _participantHealthCheckInfoCollector;
  private final DefaultMessagingService        _messagingService;
  private ZKHelixAdmin                         _managementTool;
  private final String                         _version;
  private final StateMachineEngine             _stateMachEngine;
  private int                                  _sessionTimeout;
  private PropertyStore<ZNRecord>              _propertyStore;
  private ZkHelixPropertyStore<ZNRecord>       _helixPropertyStore;
  private final List<HelixTimerTask>           _controllerTimerTasks;
  private BaseDataAccessor<ZNRecord>           _baseDataAccessor;
  List<PreConnectCallback>                     _preConnectCallbacks    =
                                                                           new LinkedList<PreConnectCallback>();
  ZKPropertyTransferServer                     _transferServer         = null;

  public ZKHelixManager(String clusterName,
                        String instanceName,
                        InstanceType instanceType,
                        String zkConnectString) throws Exception
  {
    logger.info("Create a zk-based cluster manager. clusterName:" + clusterName
        + ", instanceName:" + instanceName + ", type:" + instanceType + ", zkSvr:"
        + zkConnectString);
    int sessionTimeoutInt = -1;
    try
    {
      sessionTimeoutInt =
          Integer.parseInt(System.getProperty("zk.session.timeout", ""
              + DEFAULT_SESSION_TIMEOUT));
    }
    catch (NumberFormatException e)
    {
      logger.warn("Exception while parsing session timeout: "
          + System.getProperty("zk.session.timeout", "" + DEFAULT_SESSION_TIMEOUT));
    }
    if (sessionTimeoutInt > 0)
    {
      _sessionTimeout = sessionTimeoutInt;
    }
    else
    {
      _sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    }
    if (instanceName == null)
    {
      try
      {
        instanceName =
            InetAddress.getLocalHost().getCanonicalHostName() + "-"
                + instanceType.toString();
      }
      catch (UnknownHostException e)
      {
        // can ignore it
        logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable",
                    e);
        instanceName = "UNKNOWN";
      }
    }

    _clusterName = clusterName;
    _instanceName = instanceName;
    _instanceType = instanceType;
    _zkConnectString = zkConnectString;
    _zkStateChangeListener = new ZkStateChangeListener(this);
    _timer = null;

    _handlers = new ArrayList<CallbackHandler>();

    _messagingService = new DefaultMessagingService(this);

    _version =
        new PropertiesReader("cluster-manager-version.properties").getProperty("clustermanager.version");

    _stateMachEngine = new HelixStateMachineEngine(this);

    // add all timer tasks
    _controllerTimerTasks = new ArrayList<HelixTimerTask>();
    if (_instanceType == InstanceType.CONTROLLER)
    {
      _controllerTimerTasks.add(new HealthStatsAggregationTask(this));
    }
  }

  private boolean isInstanceSetup()
  {
    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      boolean isValid =
          _zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                                      _clusterName,
                                                      ConfigScopeProperty.PARTICIPANT.toString(),
                                                      _instanceName))
              && _zkClient.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES,
                                                             _clusterName,
                                                             _instanceName))
              && _zkClient.exists(PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                                             _clusterName,
                                                             _instanceName))
              && _zkClient.exists(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES,
                                                             _clusterName,
                                                             _instanceName))
              && _zkClient.exists(PropertyPathConfig.getPath(PropertyType.ERRORS,
                                                             _clusterName,
                                                             _instanceName));

      return isValid;
    }
    return true;
  }

  @Override
  public void addIdealStateChangeListener(final IdealStateChangeListener listener) throws Exception
  {
    logger.info("ClusterManager.addIdealStateChangeListener()");
    checkConnected();
    final String path =
        PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName);
    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeDataChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              IDEAL_STATE);
    addListener(callbackHandler);
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception
  {
    logger.info("ClusterManager.addLiveInstanceChangeListener()");
    checkConnected();
    final String path = _helixAccessor.keyBuilder().liveInstances().getPath();
    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeDataChanged, EventType.NodeChildrenChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              LIVE_INSTANCE);
    addListener(callbackHandler);
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener)
  {
    logger.info("ClusterManager.addConfigChangeListener()");
    checkConnected();
    final String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS,
                                   _clusterName,
                                   ConfigScopeProperty.PARTICIPANT.toString());

    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeChildrenChanged },
                              CONFIG);
    addListener(callbackHandler);

  }

  // TODO: Decide if do we still need this since we are exposing
  // ClusterMessagingService
  @Override
  public void addMessageListener(MessageListener listener, String instanceName)
  {
    logger.info("ClusterManager.addMessageListener() " + instanceName);
    checkConnected();
    final String path = _helixAccessor.keyBuilder().messages(instanceName).getPath();
    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeChildrenChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              MESSAGE);
    addListener(callbackHandler);
  }

  void addControllerMessageListener(MessageListener listener)
  {
    logger.info("ClusterManager.addControllerMessageListener()");
    checkConnected();
    final String path = _helixAccessor.keyBuilder().controllerMessages().getPath();

    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeChildrenChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              MESSAGES_CONTROLLER);
    addListener(callbackHandler);
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
                                            String instanceName,
                                            String sessionId)
  {
    logger.info("ClusterManager.addCurrentStateChangeListener() " + instanceName + " "
        + sessionId);
    checkConnected();
    final String path =
        _helixAccessor.keyBuilder().currentStates(instanceName, sessionId).getPath();

    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeChildrenChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              CURRENT_STATE);
    addListener(callbackHandler);
  }

  @Override
  public void addHealthStateChangeListener(HealthStateChangeListener listener,
                                           String instanceName)
  {
    // System.out.println("ZKClusterManager.addHealthStateChangeListener()");
    // TODO: re-form this for stats checking
    logger.info("ClusterManager.addHealthStateChangeListener()" + instanceName);
    checkConnected();
    final String path = _helixAccessor.keyBuilder().healthReports(instanceName).getPath();

    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] {
            EventType.NodeChildrenChanged, EventType.NodeDataChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, HEALTH);
    addListener(callbackHandler);
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {
    logger.info("ClusterManager.addExternalViewChangeListener()");
    checkConnected();
    final String path = _helixAccessor.keyBuilder().externalViews().getPath();

    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeDataChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              EXTERNAL_VIEW);
    addListener(callbackHandler);
  }

  @Override
  public DataAccessor getDataAccessor()
  {
    checkConnected();
    return _accessor;
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor()
  {
    checkConnected();
    return _helixAccessor;
  }

  @Override
  public ConfigAccessor getConfigAccessor()
  {
    checkConnected();
    return _configAccessor;
  }

  @Override
  public String getClusterName()
  {
    return _clusterName;
  }

  @Override
  public String getInstanceName()
  {
    return _instanceName;
  }

  @Override
  public void connect() throws Exception
  {
    logger.info("ClusterManager.connect()");
    if (_zkStateChangeListener.isConnected())
    {
      logger.warn("Cluster manager " + _clusterName + " " + _instanceName
          + " already connected");
      return;
    }

    try
    {
      createClient(_zkConnectString);
      _messagingService.onConnected();
    }
    catch (Exception e)
    {
      logger.error(e);
      disconnect();
      throw e;
    }
  }

  @Override
  public void disconnect()
  {

    if (!isConnected())
    {
      logger.warn("ClusterManager " + _instanceName + " already disconnected");
      return;
    }

    logger.info("disconnect " + _instanceName + "(" + _instanceType + ") from "
        + _clusterName);

    /**
     * shutdown thread pool first to avoid reset() being invoked in the middle of state
     * transition
     */
    _messagingService.getExecutor().shutDown();
    resetHandlers();

    _helixAccessor.shutdown();

    if (_leaderElectionHandler != null)
    {
      _leaderElectionHandler.reset();
    }

    if (_participantHealthCheckInfoCollector != null)
    {
      _participantHealthCheckInfoCollector.stop();
    }

    if (_timer != null)
    {
      _timer.cancel();
      _timer = null;
    }

    if (_instanceType == InstanceType.CONTROLLER)
    {
      stopTimerTasks();
    }

    if (_propertyStore != null)
    {
      _propertyStore.stop();
    }

    // unsubscribe accessor from controllerChange
    _zkClient.unsubscribeAll();

    _zkClient.close();

    // HACK seems that zkClient is not sending DISCONNECT event
    _zkStateChangeListener.disconnect();
    logger.info("Cluster manager: " + _instanceName + " disconnected");
  }

  @Override
  public String getSessionId()
  {
    checkConnected();
    return _sessionId;
  }

  @Override
  public boolean isConnected()
  {
    return _zkStateChangeListener.isConnected();
  }

  @Override
  public long getLastNotificationTime()
  {
    return -1;
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener)
  {
    checkConnected();
    final String path = _helixAccessor.keyBuilder().controller().getPath();
    logger.info("Add controller listener at: " + path);
    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeChildrenChanged,
                                  EventType.NodeDeleted, EventType.NodeCreated },
                              ChangeType.CONTROLLER);

    // System.out.println("add controller listeners to " + _instanceName +
    // " for " + _clusterName);
    // _handlers.add(callbackHandler);
    addListener(callbackHandler);
  }

  @Override
  public boolean removeListener(Object listener)
  {
    logger.info("remove listener: " + listener + " from " + _instanceName);

    synchronized (this)
    {
      Iterator<CallbackHandler> iterator = _handlers.iterator();
      while (iterator.hasNext())
      {
        CallbackHandler handler = iterator.next();
        // simply compare reference
        if (handler.getListener().equals(listener))
        {
          handler.reset();
          iterator.remove();
        }
      }
    }

    return true;
  }

  private void addLiveInstance()
  {
    LiveInstance liveInstance = new LiveInstance(_instanceName);
    liveInstance.setSessionId(_sessionId);
    liveInstance.setHelixVersion(_version);
    liveInstance.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());

    logger.info("Add live instance: InstanceName: " + _instanceName + " Session id:"
        + _sessionId);
    Builder keyBuilder = _helixAccessor.keyBuilder();
    if (!_helixAccessor.createProperty(keyBuilder.liveInstance(_instanceName),
                                       liveInstance))
    {
      String errorMsg =
          "Fail to create live instance node after waiting, so quit. instance:"
              + _instanceName;
      logger.warn(errorMsg);
      throw new HelixException(errorMsg);

    }
    String currentStatePathParent =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                   _clusterName,
                                   _instanceName,
                                   getSessionId());

    if (!_zkClient.exists(currentStatePathParent))
    {
      _zkClient.createPersistent(currentStatePathParent);
      logger.info("Creating current state path " + currentStatePathParent);
    }
  }

  private void startStatusUpdatedumpTask()
  {
    long initialDelay = 30 * 60 * 1000;
    long period = 120 * 60 * 1000;
    int timeThresholdNoChange = 180 * 60 * 1000;

    if (_timer == null)
    {
      _timer = new Timer(true);
      _timer.scheduleAtFixedRate(new ZKPathDataDumpTask(this,
                                                        _zkClient,
                                                        timeThresholdNoChange),
                                 initialDelay,
                                 period);
    }
  }

  private void createClient(String zkServers) throws Exception
  {
    String propertyStorePath =
        PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _clusterName);

    // by default use ZNRecordStreamingSerializer except for paths within the property
    // store which expects raw byte[] serialization/deserialization
    PathBasedZkSerializer zkSerializer =
        ChainedPathZkSerializer.builder(new ZNRecordStreamingSerializer())
                               .serialize(propertyStorePath, new ByteArraySerializer())
                               .build();

    _zkClient = new ZkClient(zkServers, _sessionTimeout, CONNECTIONTIMEOUT, zkSerializer);
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);

    ZkBaseDataAccessor<ZNRecord> baseDataAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    if (_instanceType == InstanceType.PARTICIPANT)
    {
      String curStatePath =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     _clusterName,
                                     _instanceName);
      _baseDataAccessor =
          new ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor,
                                                Arrays.asList(curStatePath));
    }
    else if (_instanceType == InstanceType.CONTROLLER)
    {
      String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW,

      _clusterName);
      _baseDataAccessor =
          new ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor,
                                                Arrays.asList(extViewPath));

    }
    else
    {
      _baseDataAccessor = baseDataAccessor;
    }

    _helixAccessor =
        new ZKHelixDataAccessor(_clusterName, _instanceType, _baseDataAccessor);
    _configAccessor = new ConfigAccessor(_zkClient);
    int retryCount = 0;

    _zkClient.subscribeStateChanges(_zkStateChangeListener);
    while (retryCount < RETRY_LIMIT)
    {
      try
      {
        _zkClient.waitUntilConnected(_sessionTimeout, TimeUnit.MILLISECONDS);
        _zkStateChangeListener.handleStateChanged(KeeperState.SyncConnected);
        _zkStateChangeListener.handleNewSession();
        break;
      }
      catch (HelixException e)
      {
        logger.error("fail to createClient.", e);
        throw e;
      }
      catch (Exception e)
      {
        retryCount++;

        logger.error("fail to createClient. retry " + retryCount, e);
        if (retryCount == RETRY_LIMIT)
        {
          throw e;
        }
      }
    }
  }

  private CallbackHandler createCallBackHandler(String path,
                                                Object listener,
                                                EventType[] eventTypes,
                                                ChangeType changeType)
  {
    if (listener == null)
    {
      throw new HelixException("Listener cannot be null");
    }
    return new CallbackHandler(this, _zkClient, path, listener, eventTypes, changeType);
  }

  /**
   * This will be invoked when ever a new session is created<br/>
   * 
   * case 1: the cluster manager was a participant carry over current state, add live
   * instance, and invoke message listener; case 2: the cluster manager was controller and
   * was a leader before do leader election, and if it becomes leader again, invoke ideal
   * state listener, current state listener, etc. if it fails to become leader in the new
   * session, then becomes standby; case 3: the cluster manager was controller and was NOT
   * a leader before do leader election, and if it becomes leader, instantiate and invoke
   * ideal state listener, current state listener, etc. if if fails to become leader in
   * the new session, stay as standby
   */

  protected void handleNewSession()
  {
    boolean isConnected = _zkClient.waitUntilConnected(CONNECTIONTIMEOUT, TimeUnit.MILLISECONDS);
    while (!isConnected)
    {
      logger.error("Could NOT connect to zk server in " + CONNECTIONTIMEOUT + "ms. zkServer: "
          + _zkConnectString + ", expiredSessionId: " + _sessionId + ", clusterName: "
          + _clusterName);
      isConnected = _zkClient.waitUntilConnected(CONNECTIONTIMEOUT, TimeUnit.MILLISECONDS);
    }

    ZkConnection zkConnection = ((ZkConnection) _zkClient.getConnection());
    
    synchronized (this)
    {
      _sessionId = Long.toHexString(zkConnection.getZookeeper().getSessionId());
    }
    _accessor.reset();
    _baseDataAccessor.reset();

    resetHandlers();

    logger.info("Handling new session, session id:" + _sessionId + ", instance:"
        + _instanceName + ", instanceTye: " + _instanceType + ", cluster: " + _clusterName);

    logger.info(zkConnection.getZookeeper());

    if (!ZKUtil.isClusterSetup(_clusterName, _zkClient))
    {
      throw new HelixException("Initial cluster structure is not set up for cluster:"
          + _clusterName);
    }
    if (!isInstanceSetup())
    {
      throw new HelixException("Initial cluster structure is not set up for instance:"
          + _instanceName + " instanceType:" + _instanceType);
    }

    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      handleNewSessionAsParticipant();
    }

    if (_instanceType == InstanceType.CONTROLLER
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      addControllerMessageListener(_messagingService.getExecutor());
      MessageHandlerFactory defaultControllerMsgHandlerFactory =
          new DefaultControllerMessageHandlerFactory();
      _messagingService.getExecutor()
                       .registerMessageHandlerFactory(defaultControllerMsgHandlerFactory.getMessageType(),
                                                      defaultControllerMsgHandlerFactory);
      MessageHandlerFactory defaultSchedulerMsgHandlerFactory =
          new DefaultSchedulerMessageHandlerFactory(this);
      _messagingService.getExecutor()
                       .registerMessageHandlerFactory(defaultSchedulerMsgHandlerFactory.getMessageType(),
                                                      defaultSchedulerMsgHandlerFactory);
      MessageHandlerFactory defaultParticipantErrorMessageHandlerFactory =
          new DefaultParticipantErrorMessageHandlerFactory(this);
      _messagingService.getExecutor()
                       .registerMessageHandlerFactory(defaultParticipantErrorMessageHandlerFactory.getMessageType(),
                                                      defaultParticipantErrorMessageHandlerFactory);

      if (_leaderElectionHandler == null)
      {
        final String path =
            PropertyPathConfig.getPath(PropertyType.CONTROLLER, _clusterName);

        _leaderElectionHandler =
            createCallBackHandler(path,
                                  new DistClusterControllerElection(_zkConnectString),
                                  new EventType[] { EventType.NodeChildrenChanged,
                                      EventType.NodeDeleted, EventType.NodeCreated },
                                  ChangeType.CONTROLLER);
      }
      else
      {
        _leaderElectionHandler.init();
      }
    }

    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT
        || (_instanceType == InstanceType.CONTROLLER && isLeader()))
    {
      initHandlers();
    }
  }

  private void handleNewSessionAsParticipant()
  {
    // In case there is a live instance record on zookeeper
    Builder keyBuilder = _helixAccessor.keyBuilder();

    if (_helixAccessor.getProperty(keyBuilder.liveInstance(_instanceName)) != null)
    {
      logger.warn("Found another instance with same instanceName: " + _instanceName
          + " in cluster " + _clusterName);
      // Wait for a while, in case previous storage node exits unexpectedly
      // and its liveinstance
      // still hangs around until session timeout happens
      try
      {
        Thread.sleep(_sessionTimeout + 5000);
      }
      catch (InterruptedException e)
      {
        logger.warn("Sleep interrupted while waiting for previous liveinstance to go away.",
                    e);
      }

      if (_helixAccessor.getProperty(keyBuilder.liveInstance(_instanceName)) != null)
      {
        String errorMessage =
            "instance " + _instanceName + " already has a liveinstance in cluster "
                + _clusterName;
        logger.error(errorMessage);
        throw new HelixException(errorMessage);
      }
    }
    // Invoke the PreConnectCallbacks
    for (PreConnectCallback callback : _preConnectCallbacks)
    {
      callback.onPreConnect();
    }
    addLiveInstance();
    carryOverPreviousCurrentState();

    // In case the cluster manager is running as a participant, setup message
    // listener
    _messagingService.registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                                    _stateMachEngine);
    addMessageListener(_messagingService.getExecutor(), _instanceName);
    addControllerListener(_helixAccessor);

    if (_participantHealthCheckInfoCollector == null)
    {
      _participantHealthCheckInfoCollector =
          new ParticipantHealthReportCollectorImpl(this, _instanceName);
      _participantHealthCheckInfoCollector.start();
    }
    // start the participant health check timer, also create zk path for health
    // check info
    String healthCheckInfoPath =
        _helixAccessor.keyBuilder().healthReports(_instanceName).getPath();
    if (!_zkClient.exists(healthCheckInfoPath))
    {
      _zkClient.createPersistent(healthCheckInfoPath, true);
      logger.info("Creating healthcheck info path " + healthCheckInfoPath);
    }
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback)
  {
    logger.info("Adding preconnect callback");
    _preConnectCallbacks.add(callback);
  }

  private void resetHandlers()
  {
    synchronized (this)
    {
      // get a copy of the list and iterate over the copy list
      // in case handler.reset() will modify the original handler list
      List<CallbackHandler> handlers = new ArrayList<CallbackHandler>();
      handlers.addAll(_handlers);

      for (CallbackHandler handler : handlers)
      {
        handler.reset();
        logger.info("reset handler: " + handler.getPath() + " by "
            + Thread.currentThread().getName());
      }
    }
  }

  private void initHandlers()
  {
    // may add new currentState and message listeners during init()
    // so make a copy and iterate over the copy
    synchronized (this)
    {
      List<CallbackHandler> handlers = new ArrayList<CallbackHandler>();
      handlers.addAll(_handlers);
      for (CallbackHandler handler : handlers)
      {
        handler.init();
      }
    }
  }

  private void addListener(CallbackHandler handler)
  {
    synchronized (this)
    {
      _handlers.add(handler);
      logger.info("add handler: " + handler.getPath() + " by "
          + Thread.currentThread().getName());
    }
  }

  @Override
  public boolean isLeader()
  {
    if (!isConnected())
    {
      return false;
    }

    if (_instanceType != InstanceType.CONTROLLER)
    {
      return false;
    }

    Builder keyBuilder = _helixAccessor.keyBuilder();
    LiveInstance leader = _helixAccessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null)
    {
      return false;
    }
    else
    {
      String leaderName = leader.getInstanceName();
      // TODO need check sessionId also, but in distributed mode, leader's
      // sessionId is
      // not equal to
      // the leader znode's sessionId field which is the sessionId of the
      // controller_participant that
      // successfully creates the leader node
      if (leaderName == null || !leaderName.equals(_instanceName))
      {
        return false;
      }
    }
    return true;
  }

  private void carryOverPreviousCurrentState()
  {
    Builder keyBuilder = _helixAccessor.keyBuilder();

    List<String> subPaths =
        _helixAccessor.getChildNames(keyBuilder.sessions(_instanceName));
    for (String previousSessionId : subPaths)
    {
      List<CurrentState> previousCurrentStates =
          _helixAccessor.getChildValues(keyBuilder.currentStates(_instanceName,
                                                                 previousSessionId));

      for (CurrentState previousCurrentState : previousCurrentStates)
      {
        if (!previousSessionId.equalsIgnoreCase(_sessionId))
        {
          logger.info("Carrying over old session:" + previousSessionId + " resource "
              + previousCurrentState.getId() + " to new session:" + _sessionId);
          String stateModelDefRef = previousCurrentState.getStateModelDefRef();
          if (stateModelDefRef == null)
          {
            logger.error("pervious current state doesn't have a state model def. skip it. prevCS: "
                + previousCurrentState);
            continue;
          }
          StateModelDefinition stateModel =
              _helixAccessor.getProperty(keyBuilder.stateModelDef(stateModelDefRef));
          for (String partitionName : previousCurrentState.getPartitionStateMap()
                                                          .keySet())
          {

            previousCurrentState.setState(partitionName, stateModel.getInitialState());
          }
          previousCurrentState.setSessionId(_sessionId);
          _helixAccessor.setProperty(keyBuilder.currentState(_instanceName,
                                                             _sessionId,
                                                             previousCurrentState.getId()),
                                     previousCurrentState);
        }
      }
    }
    // Deleted old current state
    for (String previousSessionId : subPaths)
    {
      if (!previousSessionId.equalsIgnoreCase(_sessionId))
      {
        String path =
            _helixAccessor.keyBuilder()
                          .currentStates(_instanceName, previousSessionId)
                          .getPath();
        logger.info("Deleting previous current state. path: " + path + "/"
            + previousSessionId);
        _zkClient.deleteRecursive(path);

      }
    }
  }

  @Deprecated
  @Override
  public synchronized PropertyStore<ZNRecord> getPropertyStore()
  {
    checkConnected();

    if (_propertyStore == null)
    {
      String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _clusterName);

      // reuse the existing zkClient because its serializer will use raw serialization
      // for paths of the property store.
      _propertyStore =
          new ZKPropertyStore<ZNRecord>(_zkClient, new ZNRecordJsonSerializer(), path);
    }

    return _propertyStore;
  }

  @Override
  public synchronized ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore()
  {
    checkConnected();

    if (_helixPropertyStore == null)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.HELIX_PROPERTYSTORE, _clusterName);

      _helixPropertyStore =
          new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkClient),
                                             path,
                                             null);
    }

    return _helixPropertyStore;
  }

  @Override
  public synchronized HelixAdmin getClusterManagmentTool()
  {
    checkConnected();
    if (_zkClient != null)
    {
      _managementTool = new ZKHelixAdmin(_zkClient);
    }
    else
    {
      logger.error("Couldn't get ZKClusterManagementTool because zkClient is null");
    }

    return _managementTool;
  }

  @Override
  public ClusterMessagingService getMessagingService()
  {
    // The caller can register message handler factories on messaging service before the
    // helix manager is connected. Thus we do not do connected check here.
    return _messagingService;
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector()
  {
    checkConnected();
    return _participantHealthCheckInfoCollector;
  }

  @Override
  public InstanceType getInstanceType()
  {
    return _instanceType;
  }

  private void checkConnected()
  {
    if (!isConnected())
    {
      throw new HelixException("ClusterManager not connected. Call clusterManager.connect()");
    }
  }

  @Override
  public String getVersion()
  {
    return _version;
  }

  @Override
  public StateMachineEngine getStateMachineEngine()
  {
    return _stateMachEngine;
  }

  protected List<CallbackHandler> getHandlers()
  {
    return _handlers;
  }

  // TODO: rename this and not expose this function as part of interface
  @Override
  public void startTimerTasks()
  {
    for (HelixTimerTask task : _controllerTimerTasks)
    {
      task.start();
    }
    startStatusUpdatedumpTask();
  }

  @Override
  public void stopTimerTasks()
  {
    for (HelixTimerTask task : _controllerTimerTasks)
    {
      task.stop();
    }
  }
}
