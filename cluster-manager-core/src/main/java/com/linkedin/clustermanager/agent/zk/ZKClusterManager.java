package com.linkedin.clustermanager.agent.zk;

import static com.linkedin.clustermanager.CMConstants.ChangeType.CONFIG;
import static com.linkedin.clustermanager.CMConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.clustermanager.CMConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.MESSAGE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.MESSAGES_CONTROLLER;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollectorImpl;
import com.linkedin.clustermanager.messaging.DefaultMessagingService;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.monitoring.ZKPathDataDumpTask;
import com.linkedin.clustermanager.participant.DistClusterControllerElection;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.tools.PropertiesReader;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKClusterManager implements ClusterManager
{
  private static Logger logger = Logger.getLogger(ZKClusterManager.class);
  private static final int RETRY_LIMIT = 3;
  private static final int CONNECTIONTIMEOUT = 10000;
  private final String _clusterName;
  private final String _instanceName;
  private final String _zkConnectString;
  private static final int SESSIONTIMEOUT = 30000;
  private ZKDataAccessor _accessor;
  private ZkClient _zkClient = null;
  private final List<CallbackHandler> _handlers;
  private final ZkStateChangeListener _zkStateChangeListener;
  private final InstanceType _instanceType;
  private String _sessionId;
  private Timer _timer;
  private CallbackHandler _leaderElectionHandler = null;
  private ParticipantHealthReportCollectorImpl _participantHealthCheckInfoCollector = null;
  private final DefaultMessagingService _messagingService;
  private ZKClusterManagementTool _managementTool = null;
  private final String _version;

  public ZKClusterManager(String clusterName, InstanceType instanceType,
      String zkConnectString) throws Exception
  {
    this(clusterName, null, instanceType, zkConnectString);
  }

  public ZKClusterManager(String clusterName, String instanceName,
      InstanceType instanceType, String zkConnectString) throws Exception
  {
    this(clusterName, instanceName, instanceType, zkConnectString, null);
  }

  public ZKClusterManager(String clusterName, String instanceName,
      InstanceType instanceType, String zkConnectString, ZkClient zkClient)
      throws Exception
  {
    logger.info("Cluster manager created: " + clusterName + " instance: "
        + instanceName + " type:" + instanceType + " zkSvr:" + zkConnectString);
    if (instanceName == null)
    {
      try
      {
        instanceName = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e)
      {
        logger
            .info(
                "Unable to get host name. Will set it to UNKNOWN, mostly ignorable",
                e);
        instanceName = "UNKNOWN";
        // can ignore it,
      }
    }

    _clusterName = clusterName;
    _instanceName = instanceName;
    this._instanceType = instanceType;
    _zkConnectString = zkConnectString;
    _zkStateChangeListener = new ZkStateChangeListener(this);
    _timer = null;
    _handlers = new ArrayList<CallbackHandler>();
    _zkClient = zkClient;
    _messagingService = new DefaultMessagingService(this);

    _version = new PropertiesReader("cluster-manager-version.properties")
        .getProperty("clustermanager.version");
  }

  private boolean isInstanceSetup()
  {
    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      boolean isValid = _zkClient.exists(PropertyPathConfig.getPath(
          PropertyType.CONFIGS, _clusterName, _instanceName))
          && _zkClient.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES,
              _clusterName, _instanceName))
          && _zkClient.exists(PropertyPathConfig.getPath(
              PropertyType.CURRENTSTATES, _clusterName, _instanceName))
          && _zkClient.exists(PropertyPathConfig.getPath(
              PropertyType.STATUSUPDATES, _clusterName, _instanceName))
          && _zkClient.exists(PropertyPathConfig.getPath(PropertyType.ERRORS,
              _clusterName, _instanceName));

      return isValid;
    }
    return true;
  }

  @Override
  public void addIdealStateChangeListener(
      final IdealStateChangeListener listener) throws Exception
  {
    checkConnected();
    final String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES,
        _clusterName);
    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeDataChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, IDEAL_STATE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener)
      throws Exception
  {
    checkConnected();
    final String path = CMUtil.getLiveInstancesPath(_clusterName);
    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, LIVE_INSTANCE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener)
  {
    checkConnected();
    final String path = CMUtil.getConfigPath(_clusterName);

    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeChildrenChanged }, CONFIG);
    _handlers.add(callbackHandler);
  }

  // TODO: Decide if do we still need this since we are exposing
  // ClusterMessagingService
  @Override
  public void addMessageListener(MessageListener listener, String instanceName)
  {
    checkConnected();
    final String path = CMUtil.getMessagePath(_clusterName, instanceName);

    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, MESSAGE);
    _handlers.add(callbackHandler);
  }

  void addControllerMessageListener(MessageListener listener)
  {
    checkConnected();
    final String path = CMUtil.getControllerPropertyPath(_clusterName,
        PropertyType.MESSAGES_CONTROLLER);

    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, MESSAGES_CONTROLLER);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addCurrentStateChangeListener(
      CurrentStateChangeListener listener, String instanceName, String sessionId)
  {
    checkConnected();
    final String path = CMUtil.getCurrentStateBasePath(_clusterName,
        instanceName) + "/" + sessionId;

    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, CURRENT_STATE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {
    checkConnected();
    final String path = CMUtil.getExternalViewPath(_clusterName);

    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeDataChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, EXTERNAL_VIEW);
    _handlers.add(callbackHandler);
  }

  @Override
  public ClusterDataAccessor getDataAccessor()
  {
    checkConnected();
    return _accessor;
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
    logger.info("Clustermanager.connect()");
    if (_zkStateChangeListener.isConnected())
    {
      logger.warn("Cluster manager " + _clusterName + " " + _instanceName
          + " already connected");
      return;
    }
    try
    {
      createClient(_zkConnectString, SESSIONTIMEOUT);
    } catch (Exception e)
    {
      logger.error(e);
      disconnect();
      throw e;
    }
  }

  @Override
  public void disconnect()
  {
    // logger.info("Cluster manager: " + _instanceName + " disconnecting");
    System.out.println("disconnect " + _instanceName + "(" + _instanceType
        + ") from " + _clusterName);

    /**
     * shutdown thread pool first to avoid reset() being invoked in the middle
     * of state transition
     */
    _messagingService.getExecutor().shutDown();

    for (CallbackHandler handler : _handlers)
    {
      handler.reset();
    }

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
    final String path = CMUtil.getControllerPath(_clusterName);
    logger.info("Add controller listener at: " + path);
    CallbackHandler callbackHandler = createCallBackHandler(path, listener,
        new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, ChangeType.CONTROLLER);

    // System.out.println("add controller listeners to " + _instanceName +
    // " for " + _clusterName);
    _handlers.add(callbackHandler);
  }

  @Override
  public boolean removeListener(Object listener)
  {
    System.out.println("remove handlers: " + _instanceName);
    removeListenerFromList(listener, _handlers);
    return true;
  }

  private void removeListenerFromList(Object listener,
      List<CallbackHandler> handlers)
  {
    if (handlers != null && handlers.size() > 0)
    {
      Iterator<CallbackHandler> iterator = handlers.iterator();
      while (iterator.hasNext())
      {
        CallbackHandler handler = iterator.next();
        // simply compare ref
        if (handler.getListener().equals(listener))
        {
          handler.reset();
          iterator.remove();
        }
      }
    }
  }

  private void addLiveInstance()
  {
    LiveInstance liveInstance = new LiveInstance(_instanceName);
    liveInstance.setSessionId(_sessionId);
    liveInstance.setClusterManagerVersion(_version);

    logger.info("Add live instance: InstanceName: " + _instanceName
        + " Session id:" + _sessionId);
    if(!_accessor.setProperty(PropertyType.LIVEINSTANCES, liveInstance,
        _instanceName))
    {
      String errorMsg = "Fail to create live instance node after waiting, so quit. instance:" + _instanceName;
      logger.warn(errorMsg);
      throw new ClusterManagerException(errorMsg);
      
    }
    String currentStatePathParent = PropertyPathConfig
        .getPath(PropertyType.CURRENTSTATES, _clusterName, _instanceName,
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
      _timer = new Timer();
      _timer.scheduleAtFixedRate(new ZKPathDataDumpTask(this, _zkClient,
          timeThresholdNoChange), initialDelay, period);
    }
  }

  private void createClient(String zkServers, int sessionTimeout)
      throws Exception
  {
    if (_zkClient == null)
    {
      ZkSerializer zkSerializer = new ZNRecordSerializer();
      _zkClient = new ZkClient(zkServers, sessionTimeout, CONNECTIONTIMEOUT,
          zkSerializer);
    }
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
    int retryCount = 0;

    _zkClient.subscribeStateChanges(_zkStateChangeListener);
    while (retryCount < RETRY_LIMIT)
    {
      try
      {
        _zkClient.waitUntilConnected(sessionTimeout, TimeUnit.MILLISECONDS);
        _zkStateChangeListener.handleStateChanged(KeeperState.SyncConnected);
        _zkStateChangeListener.handleNewSession();
        break;
      } catch (ClusterManagerException e)
      {
        throw e;
      } catch (Exception e)
      {
        retryCount++;
        // log
        if (retryCount == RETRY_LIMIT)
        {
          throw e;
        }
      }
    }
  }

  private CallbackHandler createCallBackHandler(String path, Object listener,
      EventType[] eventTypes, ChangeType changeType)
  {
    if (listener == null)
    {
      throw new ClusterManagerException("Listener cannot be null");
    }
    return new CallbackHandler(this, _zkClient, path, listener, eventTypes,
        changeType);
  }

  /**
   * This will be invoked when ever a new session is created<br/>
   *
   * case 1: the cluster manager was a participant carry over current state, add
   * live instance, and invoke message listener; case 2: the cluster manager was
   * controller and was a leader before do leader election, and if it becomes
   * leader again, invoke ideal state listener, current state listener, etc. if
   * it fails to become leader in the new session, then becomes standby; case 3:
   * the cluster manager was controller and was NOT a leader before do leader
   * election, and if it becomes leader, instantiate and invoke ideal state
   * listener, current state listener, etc. if if fails to become leader in the
   * new session, stay as standby
   */

  protected void handleNewSession()
  {
    _sessionId = UUID.randomUUID().toString();
    _accessor.reset();

    resetHandlers(_handlers);

    logger.info("Handling new session, session id:" + _sessionId
        + ", instance:" + _instanceName);

    if (!ZKUtil.isClusterSetup(_clusterName, _zkClient))
    {
      throw new ClusterManagerException(
          "Initial cluster structure is not set up for cluster:" + _clusterName);
    }
    if (!isInstanceSetup())
    {
      throw new ClusterManagerException(
          "Initial cluster structure is not set up for instance:"
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
      MessageHandlerFactory defaultControllerMsgHandlerFactory = new DefaultControllerMessageHandlerFactory();
      _messagingService.getExecutor().registerMessageHandlerFactory(
          defaultControllerMsgHandlerFactory.getMessageType(),
          defaultControllerMsgHandlerFactory);
      startStatusUpdatedumpTask();
      if (_leaderElectionHandler == null)
      {
        final String path = PropertyPathConfig.getPath(PropertyType.CONTROLLER,
            _clusterName);

        _leaderElectionHandler = createCallBackHandler(path,
            new DistClusterControllerElection(_zkConnectString),
            new EventType[]
            { EventType.NodeChildrenChanged, EventType.NodeDeleted,
                EventType.NodeCreated }, ChangeType.CONTROLLER);
      } else
      {
        _leaderElectionHandler.init();
      }
    }

    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT
        || (_instanceType == InstanceType.CONTROLLER && isLeader()))
    {
      initHandlers(_handlers);
    }
  }

  private void handleNewSessionAsParticipant()
  {
    // In case there is a live instance record on zookeeper
    if (_accessor.getProperty(PropertyType.LIVEINSTANCES, _instanceName) != null)
    {
      logger.warn("Found another instance with same instanceName: "
          + _instanceName + " in cluster " + _clusterName);
      // Wait for a while, in case previous storage node exits unexpectedly
      // and its liveinstance
      // still hangs around until session timeout happens
      try
      {
        Thread.sleep(SESSIONTIMEOUT + 5000);
      } catch (InterruptedException e)
      {
        logger.warn("Sleep interrupted while waiting for previous liveinstance to go away.", e );
      }

      if (_accessor.getProperty(PropertyType.LIVEINSTANCES, _instanceName) != null)
      {
        String errorMessage = "instance " + _instanceName
            + " already has a liveinstance in cluster " + _clusterName;
        logger.error(errorMessage);
        throw new ClusterManagerException(errorMessage);
      }
    }
    addLiveInstance();
    carryOverPreviousCurrentState();

    // In case the cluster manager is running as a participant, setup message
    // listener
    addMessageListener(_messagingService.getExecutor(), _instanceName);

    if (_participantHealthCheckInfoCollector == null)
    {
      _participantHealthCheckInfoCollector = new ParticipantHealthReportCollectorImpl(
          this, _instanceName);
      _participantHealthCheckInfoCollector.start();
    }
    // start the participant health check timer, also create zk path for health
    // check info
    String healthCheckInfoPath = CMUtil.getInstancePropertyPath(_clusterName,
        _instanceName, PropertyType.HEALTHREPORT);
    if (!_zkClient.exists(healthCheckInfoPath))
    {
      _zkClient.createPersistent(healthCheckInfoPath);
      logger.info("Creating healthcheck info path " + healthCheckInfoPath);
    }
  }

  private void resetHandlers(List<CallbackHandler> handlers)
  {
    if (handlers != null && handlers.size() > 0)
    {
      for (CallbackHandler handler : handlers)
      {
        handler.reset();
      }
    }
  }

  private void initHandlers(List<CallbackHandler> handlers)
  {
    if (handlers != null && handlers.size() > 0)
    {
      for (CallbackHandler handler : handlers)
      {
        handler.init();
      }
    }
  }

  private boolean isLeader()
  {
    LiveInstance leader = _accessor.getProperty(LiveInstance.class,
        PropertyType.LEADER);
    if (leader == null)
    {
      return false;
    } else
    {
      String leaderName = leader.getLeader();
      if (leaderName == null || !leaderName.equals(_instanceName))
      {
        return false;
      }
    }
    return true;
  }

  private void carryOverPreviousCurrentState()
  {
    List<String> subPaths = _accessor.getChildNames(PropertyType.CURRENTSTATES,
        _instanceName);
    for (String previousSessionId : subPaths)
    {
      List<CurrentState> previousCurrentStates = _accessor.getChildValues(
          CurrentState.class, PropertyType.CURRENTSTATES, _instanceName,
          previousSessionId);

      for (CurrentState previousCurrentState : previousCurrentStates)
      {
        if (!previousSessionId.equalsIgnoreCase(_sessionId))
        {
          logger.info("Carrying over old session:" + previousSessionId
              + " resource " + previousCurrentState.getId()
              + " to new session:" + _sessionId);
          String stateModelDefRef = previousCurrentState.getStateModelDefRef();
          StateModelDefinition stateModel = _accessor.getProperty(StateModelDefinition.class, PropertyType.STATEMODELDEFS, stateModelDefRef);
          for (String resourceKey : previousCurrentState
              .getResourceKeyStateMap().keySet())
          {

            previousCurrentState.setState(resourceKey, stateModel.getInitialState());
          }
          previousCurrentState.setSessionId(_sessionId);
          _accessor.setProperty(PropertyType.CURRENTSTATES,
              previousCurrentState, _instanceName, _sessionId,
              previousCurrentState.getId());
        }
      }
    }
    // Deleted old current state
    for (String previousSessionId : subPaths)
    {
      if (!previousSessionId.equalsIgnoreCase(_sessionId))
      {
        String path = CMUtil.getInstancePropertyPath(_clusterName,
            _instanceName, PropertyType.CURRENTSTATES);
        _zkClient.deleteRecursive(path + "/" + previousSessionId);
        logger.info("Deleting previous current state. path: " + path + "/"
            + previousSessionId);
      }
    }
  }

  @Override
  public PropertyStore<ZNRecord> getPropertyStore()
  {
    checkConnected();

    if (_accessor != null)
    {
      return _accessor.getPropertyStore();
    }

    return null;
  }

  @Override
  public synchronized ClusterManagementService getClusterManagmentTool()
  {
    checkConnected();
    if (_managementTool == null)
    {
      if (_zkClient != null)
      {
        _managementTool = new ZKClusterManagementTool(_zkClient);
      } else
      {
        logger.warn("_zkClient is still null");
      }
    }
    return _managementTool;
  }

  @Override
  public ClusterMessagingService getMessagingService()
  {
    checkConnected();
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
      throw new ClusterManagerException(
          "ClusterManager not connected. Call clusterManager.connect()");
    }
  }

  @Override
  public String getVersion()
  {
    return _version;
  }

}
