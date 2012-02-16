package com.linkedin.helix.manager.file;

import static com.linkedin.helix.HelixConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.LIVE_INSTANCE;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.helix.messaging.DefaultMessagingService;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.participant.HelixStateMachineEngine;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.tools.PropertiesReader;
import com.linkedin.helix.util.HelixUtil;

public class DynamicFileHelixManager implements HelixManager
{
  private static final Logger LOG = Logger.getLogger(StaticFileHelixManager.class.getName());
  private final FileDataAccessor _fileDataAccessor;

  private final String _clusterName;
  private final InstanceType _instanceType;
  private final String _instanceName;
  private boolean _isConnected;
  private final List<FileCallbackHandler> _handlers;
  private final FileHelixAdmin _mgmtTool;

  private final String _sessionId; // = "12345";
  public static final String configFile = "configFile";
  private final DefaultMessagingService _messagingService;
  private final FilePropertyStore<ZNRecord> _store;
  private final String _version;
  private final StateMachineEngine _stateMachEngine;

  public DynamicFileHelixManager(String clusterName, String instanceName,
      InstanceType instanceType, FilePropertyStore<ZNRecord> store)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _instanceType = instanceType;

    _handlers = new ArrayList<FileCallbackHandler>();

    _store = store;
    _fileDataAccessor = new FileDataAccessor(_store, clusterName); // accessor;

    _mgmtTool = new FileHelixAdmin(_store);
    _messagingService = new DefaultMessagingService(this);
    _sessionId = UUID.randomUUID().toString();
    if (instanceType == InstanceType.PARTICIPANT)
    {
      addLiveInstance();
      addMessageListener(_messagingService.getExecutor(), _instanceName);
    }

    _version = new PropertiesReader("cluster-manager-version.properties")
        .getProperty("clustermanager.version");

    _stateMachEngine = new HelixStateMachineEngine();

    _messagingService.registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
        _stateMachEngine);
  }

  @Override
  public void disconnect()
  {
    _store.stop();
    _messagingService.getExecutor().shutDown();

    _isConnected = false;
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener)
  {
    final String path = HelixUtil.getIdealStatePath(_clusterName);

    FileCallbackHandler callbackHandler = createCallBackHandler(path, listener, new EventType[] {
        EventType.NodeDataChanged, EventType.NodeDeleted, EventType.NodeCreated }, IDEAL_STATE);
    _handlers.add(callbackHandler);

  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener)
  {
    final String path = HelixUtil.getLiveInstancesPath(_clusterName);
    FileCallbackHandler callbackHandler = createCallBackHandler(path, listener, new EventType[] {
        EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated },
        LIVE_INSTANCE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addConfigChangeListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName)
  {
    final String path = HelixUtil.getMessagePath(_clusterName, instanceName);

    FileCallbackHandler callbackHandler = createCallBackHandler(path, listener, new EventType[] {
        EventType.NodeDataChanged, EventType.NodeDeleted, EventType.NodeCreated },
        ChangeType.MESSAGE);
    _handlers.add(callbackHandler);

  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
      String instanceName, String sessionId)
  {
    final String path = HelixUtil.getCurrentStateBasePath(_clusterName, instanceName) + "/"
        + sessionId;

    FileCallbackHandler callbackHandler = createCallBackHandler(path, listener, new EventType[] {
        EventType.NodeChildrenChanged, EventType.NodeDeleted, EventType.NodeCreated },
        CURRENT_STATE);

    _handlers.add(callbackHandler);
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addExternalViewChangeListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public DataAccessor getDataAccessor()
  {
    return _fileDataAccessor;
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
  public void connect()
  {
    _store.start();
    _isConnected = true;
  }

  @Override
  public String getSessionId()
  {
    return _sessionId;
  }

  @Override
  public boolean isConnected()
  {
    return _isConnected;
  }

  private boolean isClusterSetup(String clusterName)
  {
    if (clusterName == null || _store == null)
    {
      return false;
    }

    boolean isValid = _store.exists(PropertyPathConfig.getPath(PropertyType.IDEALSTATES,
        clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.INSTANCES, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER,
            clusterName))
        && _store.exists(PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName));

    return isValid;
  }

  private boolean isInstanceSetup()
  {
    if (_instanceType == InstanceType.PARTICIPANT
        || _instanceType == InstanceType.CONTROLLER_PARTICIPANT)
    {
      boolean isValid = _store.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS,
          _clusterName, _instanceName))
          && _store.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES, _clusterName,
              _instanceName))
          && _store.exists(PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, _clusterName,
              _instanceName))
          && _store.exists(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES, _clusterName,
              _instanceName))
          && _store.exists(PropertyPathConfig.getPath(PropertyType.ERRORS, _clusterName,
              _instanceName));

      return isValid;
    }
    return true;
  }

  private void addLiveInstance()
  {
    if (!isClusterSetup(_clusterName))
    {
      throw new HelixException("Initial cluster structure is not set up for cluster:"
          + _clusterName);
    }

    if (!isInstanceSetup())
    {
      throw new HelixException("Instance is not configured for instance:" + _instanceName
          + " instanceType:" + _instanceType);
    }

    LiveInstance liveInstance = new LiveInstance(_instanceName);
    liveInstance.setSessionId(_sessionId);
    _fileDataAccessor.setProperty(PropertyType.LIVEINSTANCES, liveInstance.getRecord(),
        _instanceName);
  }

  @Override
  public long getLastNotificationTime()
  {
    return 0;
  }

  @Override
  public void addControllerListener(ControllerChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addControllerListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public boolean removeListener(Object listener)
  {
    // TODO Auto-generated method stub
    return false;
  }

  private FileCallbackHandler createCallBackHandler(String path, Object listener,
      EventType[] eventTypes, ChangeType changeType)
  {
    if (listener == null)
    {
      throw new HelixException("Listener cannot be null");
    }
    return new FileCallbackHandler(this, path, listener, eventTypes, changeType);
  }

  @Override
  public HelixAdmin getClusterManagmentTool()
  {
    return _mgmtTool;
  }

  @Override
  public PropertyStore<ZNRecord> getPropertyStore()
  {
    if (_fileDataAccessor != null)
    {
      return _fileDataAccessor.getPropertyStore();
    }
    return null;
  }

  @Override
  public ClusterMessagingService getMessagingService()
  {
    return _messagingService;
  }

  @Override
  public ParticipantHealthReportCollector getHealthReportCollector()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InstanceType getInstanceType()
  {
    return _instanceType;
  }

  @Override
  public void addHealthStateChangeListener(HealthStateChangeListener listener, String instanceName)
      throws Exception
  {
    // TODO Auto-generated method stub

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

}
