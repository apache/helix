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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HealthStateChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.MessageListener;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

public abstract class AbstractManager implements HelixManager, IZkStateListener {
  private static Logger LOG = Logger.getLogger(AbstractManager.class);

  final String _zkAddress;
  final String _clusterName;
  final String _instanceName;
  final InstanceType _instanceType;
  final int _sessionTimeout;
  final List<PreConnectCallback> _preConnectCallbacks;
  protected final List<CallbackHandler> _handlers;
  final HelixManagerProperties _properties;
  
  /**
   * helix version#
   */
  final String _version;

  protected ZkClient _zkclient = null;
  final DefaultMessagingService _messagingService;

  BaseDataAccessor<ZNRecord> _baseDataAccessor;
  ZKHelixDataAccessor _dataAccessor;
  final Builder _keyBuilder;
  ConfigAccessor _configAccessor;
  ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  LiveInstanceInfoProvider _liveInstanceInfoProvider = null;
  final List<HelixTimerTask> _timerTasks = new ArrayList<HelixTimerTask>();


  volatile String _sessionId;
  
  /**
   * Keep track of timestamps that zk State has become Disconnected
   * If in a _timeWindowLengthMs window zk State has become Disconnected 
   * for more than_maxDisconnectThreshold times disconnect the zkHelixManager
   */
  final List<Long> _disconnectTimeHistory = new LinkedList<Long>();
   
  final int _flappingTimeWindowMs; 
  final int _maxDisconnectThreshold;

  
  public AbstractManager(String zkAddress, String clusterName, String instanceName,
      InstanceType instanceType) {
    
    LOG.info("Create a zk-based cluster manager. zkSvr: " + zkAddress + ", clusterName: " + clusterName
        + ", instanceName: " + instanceName + ", type: " + instanceType);

    _zkAddress = zkAddress;
    _clusterName = clusterName;
    _instanceType = instanceType;
    _instanceName = instanceName;
    _preConnectCallbacks = new LinkedList<PreConnectCallback>();
    _handlers = new ArrayList<CallbackHandler>();
    _properties = new HelixManagerProperties("cluster-manager-version.properties");
    _version = _properties.getVersion();

    _keyBuilder = new Builder(clusterName);
    _messagingService = new DefaultMessagingService(this);

    
    /**
     * use system property if available
     */
    _flappingTimeWindowMs = getSystemPropertyAsInt("helixmanager.flappingTimeWindow", 
        ZKHelixManager.FLAPPING_TIME_WINDIOW);
    
    _maxDisconnectThreshold = getSystemPropertyAsInt("helixmanager.maxDisconnectThreshold", 
         ZKHelixManager.MAX_DISCONNECT_THRESHOLD);
 
    _sessionTimeout = getSystemPropertyAsInt("zk.session.timeout", ZkClient.DEFAULT_SESSION_TIMEOUT);

  }
  
  private int getSystemPropertyAsInt(String propertyKey, int propertyDefaultValue) {
    String valueString = System.getProperty(propertyKey, "" + propertyDefaultValue);
    
    try
    {
      int value = Integer.parseInt(valueString);
      if (value > 0) {
        return value;
      }
    }
    catch (NumberFormatException e)
    {
      LOG.warn("Exception while parsing property: " + propertyKey + ", string: " + valueString
          + ", using default value: " + propertyDefaultValue);
    }
    
    return propertyDefaultValue;
  }
  
  /**
   * different types of helix manager should impl its own handle new session logic
   */
//  public abstract void handleNewSession();
  
  @Override
  public void connect() throws Exception {
    LOG.info("ClusterManager.connect()");
    if (isConnected())
    {
      LOG.warn("Cluster manager: " + _instanceName + " for cluster: " + _clusterName
          + " already connected. skip connect");
      return;
    }

    try
    {
      createClient();
      _messagingService.onConnected();
    }
    catch (Exception e)
    {
      LOG.error("fail to connect " + _instanceName, e);
      disconnect();
      throw e;
    } 
  }

  @Override
  public boolean isConnected() {
    if (_zkclient == null) {
      return false;
    }
    ZkConnection zkconnection = (ZkConnection) _zkclient.getConnection();
    if (zkconnection != null) {
      States state = zkconnection.getZookeeperState();
      return state == States.CONNECTED;
    }
    return false;
  }

  /**
   * specific disconnect logic for each helix-manager type
   */
  abstract void doDisconnect();
  
  /**
   * This function can be called when the connection are in bad state(e.g. flapping), 
   * in which isConnected() could be false and we want to disconnect from cluster.
   */
  @Override
  public void disconnect() {
    LOG.info("disconnect " + _instanceName + "(" + _instanceType + ") from "
        + _clusterName);

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
      resetHandlers();
  
      _dataAccessor.shutdown();
  
      doDisconnect();
  
      _zkclient.unsubscribeAll();
    } finally {
      _zkclient.close();
      LOG.info("Cluster manager: " + _instanceName + " disconnected");
    }    
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).idealStates(), ChangeType.IDEAL_STATE, 
        new EventType[] { EventType.NodeDataChanged, EventType.NodeDeleted, EventType.NodeCreated });    
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).liveInstances(), ChangeType.LIVE_INSTANCE, 
        new EventType[] { EventType.NodeDataChanged, EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });    
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).instanceConfigs(), ChangeType.INSTANCE_CONFIG, 
        new EventType[] { EventType.NodeChildrenChanged });    
  }

  @Override
  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener)
      throws Exception {
    addListener(listener, new Builder(_clusterName).instanceConfigs(), ChangeType.INSTANCE_CONFIG, 
        new EventType[] { EventType.NodeChildrenChanged });    
  }

  @Override
  public void addConfigChangeListener(ScopedConfigChangeListener listener, ConfigScopeProperty scope)
      throws Exception {
    Builder keyBuilder = new Builder(_clusterName);

    PropertyKey propertyKey = null;
    switch(scope)
    {
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

    if (propertyKey != null)
    {
      addListener(listener, propertyKey, ChangeType.CONFIG, 
          new EventType[] { EventType.NodeChildrenChanged });
    } else
    {
      LOG.error("Can't add listener to config scope: " + scope);
    }    
  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) {
    addListener(listener, new Builder(_clusterName).messages(instanceName), ChangeType.MESSAGE, 
        new EventType[] { EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });    
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId) throws Exception {
    addListener(listener, new Builder(_clusterName).currentStates(instanceName, sessionId), ChangeType.CURRENT_STATE,
        new EventType[] { EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });    
  }

  @Override
  public void addHealthStateChangeListener(HealthStateChangeListener listener, String instanceName)
      throws Exception {
    addListener(listener, new Builder(_clusterName).healthReports(instanceName), ChangeType.HEALTH,
        new EventType[] { EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });    
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    addListener(listener, new Builder(_clusterName).externalViews(), ChangeType.EXTERNAL_VIEW,
        new EventType[] { EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });    
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener) {
    addListener(listener, new Builder(_clusterName).controller(), ChangeType.CONTROLLER,
        new EventType[] { EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });
  }

  void addControllerMessageListener(MessageListener listener)
  {
    addListener(listener, new Builder(_clusterName).controllerMessages(), ChangeType.MESSAGES_CONTROLLER,
          new EventType[] { EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated });
  }

  @Override
  public boolean removeListener(PropertyKey key, Object listener) {
    LOG.info("Removing listener: " + listener + " on path: " + key.getPath() 
        + " from cluster: " + _clusterName + " by instance: " + _instanceName);

    synchronized (this)
    {
      List<CallbackHandler> toRemove = new ArrayList<CallbackHandler>();
      for (CallbackHandler handler : _handlers)
      {
        // compare property-key path and listener reference
        if (handler.getPath().equals(key.getPath()) && handler.getListener().equals(listener))
        {
          toRemove.add(handler);
        }
      }
      
      _handlers.removeAll(toRemove);
      
      // handler.reset() may modify the handlers list, so do it outside the iteration
      for (CallbackHandler handler : toRemove) {
        handler.reset();
      }
    }

    return true;  
  }

  @Override
  public HelixDataAccessor getHelixDataAccessor() {
    checkConnected();
    return _dataAccessor;
  }

  @Override
  public ConfigAccessor getConfigAccessor() {
    checkConnected();
    return _configAccessor;
  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }

  @Override
  public String getInstanceName() {
    return _instanceName;
  }

  @Override
  public String getSessionId() {
    checkConnected();
    return _sessionId;
  }

  @Override
  public long getLastNotificationTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HelixAdmin getClusterManagmentTool() {
    checkConnected();
    if (_zkclient != null)
    {
      return new ZKHelixAdmin(_zkclient);
    }
    
    LOG.error("Couldn't get ZKClusterManagementTool because zkclient is null");
    return null;
  }

  @Override
  public synchronized ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    checkConnected();

    if (_helixPropertyStore == null)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _clusterName);

      _helixPropertyStore =
          new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkclient),
                                             path,
                                             null);
    }

    return _helixPropertyStore;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    // The caller can register message handler factories on messaging service before the
    // helix manager is connected. Thus we do not check connected here
    return _messagingService;
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector() {
    // helix-participant will override this
    return null;
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
    // helix-participant will override this
    return null;
  }

  @Override
  public abstract boolean isLeader();

  @Override
  public void startTimerTasks() {
    for (HelixTimerTask task : _timerTasks)
    {
      task.start();
    }

  }

  @Override
  public void stopTimerTasks() {
    for (HelixTimerTask task : _timerTasks)
    {
      task.stop();
    }
    
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    LOG.info("Adding preconnect callback: " + callback);
    _preConnectCallbacks.add(callback);    
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
  protected void waitUntilConnected() {
    boolean isConnected;
    do {
      isConnected = _zkclient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT,
          TimeUnit.MILLISECONDS);
      if (!isConnected) {
        LOG.error("fail to connect zkserver: " + _zkAddress + " in "
            + ZkClient.DEFAULT_CONNECTION_TIMEOUT + "ms. expiredSessionId: " + _sessionId
            + ", clusterName: " + _clusterName);
        continue;
      }
      
      ZkConnection zkConnection = ((ZkConnection) _zkclient.getConnection());
      _sessionId = Long.toHexString(zkConnection.getZookeeper().getSessionId());

      /**
       * at the time we read session-id, zkconnection might be lost again
       * wait until we get a non-zero session-id
       */
    } while ("0".equals(_sessionId));
    
    LOG.info("Handling new session, session id: " + _sessionId
        + ", instance: " + _instanceName + ", instanceTye: " + _instanceType
        + ", cluster: " + _clusterName
        + ", zkconnection: " + ((ZkConnection) _zkclient.getConnection()).getZookeeper());
  }
  
  protected void checkConnected()
  {
    if (!isConnected())
    {
      throw new HelixException("ClusterManager not connected. Call clusterManager.connect()");
    }
  }

  protected void addListener(Object listener, PropertyKey propertyKey, ChangeType changeType, EventType[] eventType)
  {
    checkConnected();

    PropertyType type = propertyKey.getType();

    synchronized (this)
    {
      for (CallbackHandler handler : _handlers)
      {
        // compare property-key path and listener reference
        if (handler.getPath().equals(propertyKey.getPath()) && handler.getListener().equals(listener))
        {
          LOG.info("Listener: " + listener + " on path: " + propertyKey.getPath() + " already exists. skip add");
          return;
        }
      }
      
      CallbackHandler newHandler = new CallbackHandler(this, _zkclient, propertyKey, 
          listener, eventType, changeType);
      
      _handlers.add(newHandler);
      LOG.info("Added listener: " + listener + " for type: " + type + " to path: " + newHandler.getPath());
    }
  }
  
  protected void initHandlers(List<CallbackHandler> handlers)
  {
    synchronized (this)
    {
      if (handlers != null)
      {
        for (CallbackHandler handler : handlers)
        {
          handler.init();
          LOG.info("init handler: " + handler.getPath() + ", " + handler.getListener());
        }
      }
    }
  }
  
  protected void resetHandlers()
  {
    synchronized (this)
    {
      if (_handlers != null) {
        // get a copy of the list and iterate over the copy list
        // in case handler.reset() modify the original handler list
        List<CallbackHandler> tmpHandlers = new ArrayList<CallbackHandler>();
        tmpHandlers.addAll(_handlers);
  
        for (CallbackHandler handler : tmpHandlers)
        {
          handler.reset();
          LOG.info("reset handler: " + handler.getPath() + ", " + handler.getListener());
        }
      }
    }
  }

  /**
   * different helix-manager may override this to have a cache-enabled based-data-accessor
   * 
   * @param baseDataAccessor
   * @return
   */
  BaseDataAccessor<ZNRecord> createBaseDataAccessor(ZkBaseDataAccessor<ZNRecord> baseDataAccessor) {
    return baseDataAccessor;
  }
  
  void createClient() throws Exception {
    PathBasedZkSerializer zkSerializer =
        ChainedPathZkSerializer.builder(new ZNRecordStreamingSerializer())
                               .build();

    _zkclient = new ZkClient(_zkAddress, _sessionTimeout, ZkClient.DEFAULT_CONNECTION_TIMEOUT, zkSerializer);

    ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    _baseDataAccessor = createBaseDataAccessor(baseDataAccessor);

    _dataAccessor =
        new ZKHelixDataAccessor(_clusterName, _instanceType, _baseDataAccessor);
    _configAccessor = new ConfigAccessor(_zkclient);
    
    int retryCount = 0;

    _zkclient.subscribeStateChanges(this);
    while (retryCount < 3)
    {
      try
      {
        _zkclient.waitUntilConnected(_sessionTimeout, TimeUnit.MILLISECONDS);
        handleStateChanged(KeeperState.SyncConnected);
        handleNewSession();
        break;
      }
      catch (HelixException e)
      {
        LOG.error("fail to createClient.", e);
        throw e;
      }
      catch (Exception e)
      {
        retryCount++;

        LOG.error("fail to createClient. retry " + retryCount, e);
        if (retryCount == 3)
        {
          throw e;
        }
      }
    }
  }

  // TODO separate out flapping detection code
  @Override
  public void handleStateChanged(KeeperState state) throws Exception {
    switch (state)
    {
    case SyncConnected:
      ZkConnection zkConnection = (ZkConnection) _zkclient.getConnection();
      LOG.info("KeeperState: " + state + ", zookeeper:" + zkConnection.getZookeeper());
      break;
    case Disconnected:
      LOG.info("KeeperState:" + state + ", disconnectedSessionId: "
          + _sessionId + ", instance: " + _instanceName + ", type: "
          + _instanceType);

      /**
       * Track the time stamp that the disconnected happens, then check history and see if
       * we should disconnect the helix-manager
       */
      _disconnectTimeHistory.add(System.currentTimeMillis());
      if (isFlapping())
      {
        LOG.error("instanceName: " + _instanceName + " is flapping. diconnect it. "
          + " maxDisconnectThreshold: " + _maxDisconnectThreshold 
          + " disconnects in " + _flappingTimeWindowMs + "ms."); 
        disconnect();
      }
      break;
    case Expired:
      LOG.info("KeeperState:" + state + ", expiredSessionId: " + _sessionId + ", instance: "
          + _instanceName + ", type: " + _instanceType);
      break;
    default:
      break;
    }    
  }
  
  /**
   * If zk state has changed into Disconnected for _maxDisconnectThreshold times during previous _timeWindowLengthMs Ms
   * time window, we think that there are something wrong going on and disconnect the zkHelixManager from zk.
   */
  private boolean isFlapping()
  {
    if(_disconnectTimeHistory.size() == 0)
    {
      return false;
    }
    long mostRecentTimestamp = _disconnectTimeHistory.get(_disconnectTimeHistory.size() - 1);
    
    // Remove disconnect history timestamp that are older than _flappingTimeWindowMs ago
    while ((_disconnectTimeHistory.get(0) + _flappingTimeWindowMs) < mostRecentTimestamp)
    {
      _disconnectTimeHistory.remove(0);
    }
    return _disconnectTimeHistory.size() > _maxDisconnectThreshold;
  }

  /**
   * controller should override it to return a list of timers that need to start/stop when leadership changes
   * 
   * @return
   */
  protected List<HelixTimerTask> getControllerHelixTimerTasks() {
    return null;
  }
}
