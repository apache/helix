package com.linkedin.clustermanager.agent.zk;

import static com.linkedin.clustermanager.CMConstants.ChangeType.CONFIG;
import static com.linkedin.clustermanager.CMConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.clustermanager.CMConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.MESSAGE;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.CMConstants.ZNAttribute;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
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
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollectorImpl;
import com.linkedin.clustermanager.messaging.DefaultMessagingService;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.monitoring.ZKPathDataDumpTask;
import com.linkedin.clustermanager.participant.DistClusterControllerElection;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKClusterManager implements ClusterManager
{
  private static Logger                        logger                               =
                                                                                        Logger.getLogger(ZKClusterManager.class);
  private static final int                     RETRY_LIMIT                          = 3;
  private static final int                     CONNECTIONTIMEOUT                    = 10000;
  private final String                         _clusterName;
  private final String                         _instanceName;
  private final String                         _zkConnectString;
  private static final int                     SESSIONTIMEOUT                       = 30000;
  private ZKDataAccessor                       _accessor;
  private ZkClient                             _zkClient                            = null;
  private final List<CallbackHandler>          _handlers;
  private final ZkStateChangeListener          _zkStateChangeListener;
  private final InstanceType                   _instanceType;
  private String                               _sessionId;
  private Timer                                _timer;
  private CallbackHandler                      _leaderElectionHandler               = null;
  private ParticipantHealthReportCollectorImpl _participantHealthCheckInfoCollector = null;
  private final DefaultMessagingService        _messagingService;
  private ZKClusterManagementTool              _managementTool                      = null;

  public ZKClusterManager(String clusterName, InstanceType instanceType, String zkConnectString) throws Exception
  {
    this(clusterName, null, instanceType, zkConnectString);
  }

  public ZKClusterManager(String clusterName,
                          String instanceName,
                          InstanceType instanceType,
                          String zkConnectString) throws Exception
  {
    this(clusterName, instanceName, instanceType, zkConnectString, null);
  }

  public ZKClusterManager(String clusterName,
                          String instanceName,
                          InstanceType instanceType,
                          String zkConnectString,
                          ZkClient zkClient) throws Exception
  {
    logger.info("Cluster manager created: " + clusterName + " instance: " + instanceName + " type:"
        + instanceType + " zkSvr:" + zkConnectString);
    if (instanceName == null)
    {
      try
      {
        instanceName = InetAddress.getLocalHost().getCanonicalHostName();
      }
      catch (UnknownHostException e)
      {
        logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
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
    connect();
  }

  private boolean isInstanceSetup()
  {
    if (_instanceType == InstanceType.PARTICIPANT)
    {
      boolean isValid =
          _zkClient.exists(CMUtil.getConfigPath(_clusterName, _instanceName))
              && _zkClient.exists(CMUtil.getMessagePath(_clusterName, _instanceName))
              && _zkClient.exists(CMUtil.getCurrentStateBasePath(_clusterName, _instanceName))
              && _zkClient.exists(CMUtil.getStatusUpdatesPath(_clusterName, _instanceName))
              && _zkClient.exists(CMUtil.getErrorsPath(_clusterName, _instanceName));
      return isValid;
    }
    return true;
  }

  @Override
  public void addIdealStateChangeListener(final IdealStateChangeListener listener) throws Exception
  {
    final String path = CMUtil.getIdealStatePath(_clusterName);
    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] { EventType.NodeDataChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, IDEAL_STATE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception
  {
    final String path = CMUtil.getLiveInstancesPath(_clusterName);
    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] { EventType.NodeChildrenChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, LIVE_INSTANCE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener)
  {
    final String path = CMUtil.getConfigPath(_clusterName);

    CallbackHandler callbackHandler =
        createCallBackHandler(path,
                              listener,
                              new EventType[] { EventType.NodeChildrenChanged },
                              CONFIG);
    _handlers.add(callbackHandler);
  }

  // TODO: Decide if do we still need this since we are exposing ClusterMessagingService
  @Override
  public void addMessageListener(MessageListener listener, String instanceName)
  {
    final String path = CMUtil.getMessagePath(_clusterName, instanceName);

    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] { EventType.NodeChildrenChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, MESSAGE);
    _handlers.add(callbackHandler);
  }
  
  void addControllerMessageListener(MessageListener listener)
  {
    final String path = CMUtil.getControllerPropertyPath(_clusterName, ControllerPropertyType.MESSAGES);
    
    CallbackHandler callbackHandler =
      createCallBackHandler(path, listener, new EventType[] { EventType.NodeChildrenChanged,
          EventType.NodeDeleted, EventType.NodeCreated }, MESSAGE);
    _handlers.add(callbackHandler);
  }
  
  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
                                            String instanceName,
                                            String sessionId)
  {
    final String path =
        CMUtil.getCurrentStateBasePath(_clusterName, instanceName) + "/" + sessionId;

    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] { EventType.NodeChildrenChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, CURRENT_STATE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {

    final String path = CMUtil.getExternalViewPath(_clusterName);

    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] { EventType.NodeDataChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, EXTERNAL_VIEW);
    _handlers.add(callbackHandler);
  }

  @Override
  public ClusterDataAccessor getDataAccessor()
  {
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
      return;
    }
    
    try
    {
      createClient(_zkConnectString, SESSIONTIMEOUT);
  
      if (!isClusterSetup())
      {
        throw new Exception("Initial cluster structure is not set up for cluster:" + _clusterName);
      }
      if (!isInstanceSetup())
      {
        throw new Exception("Initial cluster structure is not set up for instance:" + _instanceName
            + " instanceType:" + _instanceType);
      }
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void disconnect()
  {
    logger.info("Cluster manager: " + _instanceName + " disconnected");

    for (CallbackHandler handler : _handlers)
    {
      handler.reset();
    }

    if (_leaderElectionHandler != null)
    {
      DistClusterControllerElection listener =
          (DistClusterControllerElection) _leaderElectionHandler.getListener();
      ClusterManager leader = listener.getLeader();
      if (leader != null)
      {
        leader.disconnect();
      }

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

  }

  @Override
  public String getSessionId()
  {
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
    // return _zkClient.;
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener)
  {
    final String path = CMUtil.getControllerPath(_clusterName);
    logger.info("Add controller listener at: " + path);
    CallbackHandler callbackHandler =
        createCallBackHandler(path, listener, new EventType[] { EventType.NodeChildrenChanged,
            EventType.NodeDeleted, EventType.NodeCreated }, ChangeType.CONTROLLER);
    _handlers.add(callbackHandler);
  }

  @Override
  public boolean removeListener(Object listener)
  {
    removeListenerFromList(listener, _handlers);
    return true;
  }

  private void removeListenerFromList(Object listener, List<CallbackHandler> handlers)
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
    ZNRecord metaData = new ZNRecord();
    // set it from the session
    metaData.setId(_instanceName);
    metaData.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(), _sessionId);

    logger.info("Add live instance: InstanceName: " + _instanceName + " Session id:" + _sessionId);

    _accessor.setClusterProperty(ClusterPropertyType.LIVEINSTANCES,
                                 _instanceName,
                                 metaData,
                                 CreateMode.EPHEMERAL);
    String currentStatePathParent =
        CMUtil.getCurrentStateBasePath(_clusterName, _instanceName) + "/" + getSessionId();
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
    String path =
        CMUtil.getInstancePropertyPath(_clusterName,
                                       _instanceName,
                                       InstancePropertyType.STATUSUPDATES);
    List<String> paths = new ArrayList<String>();
    paths.add(path);
    if (_timer == null)
    {
      _timer = new Timer();
      _timer.scheduleAtFixedRate(new ZKPathDataDumpTask(_zkClient, paths, timeThresholdNoChange),
                                 initialDelay,
                                 period);
    }
  }

  private void createClient(String zkServers, int sessionTimeout) throws Exception
  {
    if (_zkClient == null)
    {
      ZkSerializer zkSerializer = new ZNRecordSerializer();
      _zkClient = new ZkClient(zkServers, sessionTimeout, CONNECTIONTIMEOUT, zkSerializer);
    }
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
    int retryCount = 0;
    
    _zkClient.subscribeStateChanges(_zkStateChangeListener);
    while (retryCount < RETRY_LIMIT)
    {
      try
      {
        _zkClient.waitUntilConnected(sessionTimeout, TimeUnit.MILLISECONDS);
        _zkStateChangeListener.handleNewSession();
        _zkStateChangeListener.handleStateChanged(KeeperState.SyncConnected);
        break;
      }
      catch (ClusterManagerException e)
      {
        throw e;
      }
      catch (Exception e)
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

  private CallbackHandler createCallBackHandler(String path,
                                                Object listener,
                                                EventType[] eventTypes,
                                                ChangeType changeType)
  {
    if (listener == null)
    {
      throw new ClusterManagerException("Listener cannot be null");
    }
    return new CallbackHandler(this, _zkClient, path, listener, eventTypes, changeType);
  }

  private boolean isClusterSetup()
  {
    String idealStatePath = CMUtil.getIdealStatePath(_clusterName);
    boolean isValid =
        _zkClient.exists(idealStatePath) && _zkClient.exists(CMUtil.getConfigPath(_clusterName))
            && _zkClient.exists(CMUtil.getLiveInstancesPath(_clusterName))
            && _zkClient.exists(CMUtil.getMemberInstancesPath(_clusterName))
            && _zkClient.exists(CMUtil.getExternalViewPath(_clusterName));
    return isValid;
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

  protected void handleNewSession() throws Exception
  {
    _sessionId = UUID.randomUUID().toString();
    resetHandlers(_handlers);

    logger.info("Handling new session, session id:" + _sessionId + ", instance:" + _instanceName);

    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      handleNewSessionAsParticipant();
    }

    if (_instanceType == InstanceType.CONTROLLER
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      addControllerMessageListener( _messagingService.getExecutor());
      MessageHandlerFactory defaultControllerMsgHandlerFactory = new DefaultControllerMessageHandlerFactory();
      _messagingService.getExecutor().registerMessageHandlerFactory(
          defaultControllerMsgHandlerFactory.getMessageType(), defaultControllerMsgHandlerFactory);
      if (_leaderElectionHandler == null)
      {
        final String path = CMUtil.getControllerPath(_clusterName);

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
      initHandlers(_handlers);
    }
  }

  private void handleNewSessionAsParticipant()
  {
    // In case there is a live instance record on zookeeper
    if (_accessor.getClusterProperty(ClusterPropertyType.LIVEINSTANCES, _instanceName) != null)
    {
      logger.warn("find liveinstance record for " + _instanceName + " in cluster " + _clusterName);
      // Wait for a while, in case previous storage node exits unexpectedly
      // and its liveinstance
      // still hangs around until session timeout happens
      try
      {
        Thread.sleep(SESSIONTIMEOUT + 5000);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
      if (_accessor.getClusterProperty(ClusterPropertyType.LIVEINSTANCES, _instanceName) != null)
      {
        String errorMessage =
            "instance " + _instanceName + " already has a liveinstance in cluster " + _clusterName;
        logger.error(errorMessage);
        throw new ClusterManagerException(errorMessage);
      }
    }
    carryOverPreviousCurrentState();
    addLiveInstance();
    startStatusUpdatedumpTask();

    // In case the cluster manager is running as a participant, setup message listener
    addMessageListener(_messagingService.getExecutor(), _instanceName);

    if (_participantHealthCheckInfoCollector == null)
    {
      _participantHealthCheckInfoCollector =
          new ParticipantHealthReportCollectorImpl(this, _instanceName);
      _participantHealthCheckInfoCollector.start();
    }
    // start the participant health check timer, also create zk path for health check info
    String healthCheckInfoPath =
        CMUtil.getInstancePropertyPath(_clusterName,
                                       _instanceName,
                                       InstancePropertyType.HEALTHREPORT);
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
    boolean ret = true;

    ZNRecord leaderRecord = _accessor.getControllerProperty(ControllerPropertyType.LEADER);
    if (leaderRecord == null)
    {
      ret = false;
    }
    else
    {
      String leader = leaderRecord.getSimpleField(ControllerPropertyType.LEADER.toString());

      if (leader == null || !leader.equals(_instanceName))
      {
        ret = false;
      }
    }
    return ret;
  }

  private void carryOverPreviousCurrentState()
  {
    List<String> subPaths =
        _accessor.getInstancePropertySubPaths(_instanceName, InstancePropertyType.CURRENTSTATES);
    for (String previousSessionId : subPaths)
    {
      List<ZNRecord> previousCurrentStates =
          _accessor.getInstancePropertyList(_instanceName,
                                            previousSessionId,
                                            InstancePropertyType.CURRENTSTATES);
      for (ZNRecord previousCurrentState : previousCurrentStates)
      {
        if (!previousSessionId.equalsIgnoreCase(_sessionId))
        {
          logger.info("Carrying over old session:" + previousSessionId + " resource "
              + previousCurrentState.getId() + " to new session:" + _sessionId);
          for (String resourceKey : previousCurrentState.mapFields.keySet())
          {
            previousCurrentState.getMapField(resourceKey).put(ZNAttribute.CURRENT_STATE.toString(),
                                                              "OFFLINE");
          }
          previousCurrentState.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(),
                                              _sessionId);
          _accessor.setInstanceProperty(_instanceName,
                                        InstancePropertyType.CURRENTSTATES,
                                        _sessionId,
                                        previousCurrentState.getId(),
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
            CMUtil.getInstancePropertyPath(_clusterName,
                                           _instanceName,
                                           InstancePropertyType.CURRENTSTATES);
        _zkClient.deleteRecursive(path + "/" + previousSessionId);
        logger.info("Deleting previous current state. path: " + path + "/" + previousSessionId);
      }
    }
  }

  @Override
  public <T> PropertyStore<T> getPropertyStore(String rootNamespace,
                                               PropertySerializer<T> serializer)
  {
    String path = "/" + _clusterName + "/" + "PRPOPERTY_STORE" + "/" + rootNamespace;
    if (!_zkClient.exists(path))
    {
      _zkClient.createPersistent(path);
    }

    return new ZKPropertyStore<T>((ZkConnection) _zkClient.getConnection(), serializer, path);
  }

  @Override
  public synchronized ClusterManagementService getClusterManagmentTool()
  {
    if (_managementTool == null)
    {
      if (_zkClient != null)
      {
        _managementTool = new ZKClusterManagementTool(_zkClient);
      }
      else
      {
        logger.warn("_zkClient is still null");
      }
    }
    return _managementTool;
  }

  @Override
  public ClusterMessagingService getMessagingService()
  {
    return _messagingService;
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector()
  {
    return _participantHealthCheckInfoCollector;
  }

  @Override
  public InstanceType getInstanceType()
  {
    return _instanceType;
  }

}
