package com.linkedin.helix.agent.file;

import static com.linkedin.helix.CMConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.helix.CMConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.helix.CMConstants.ChangeType.LIVE_INSTANCE;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManagementService;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterManagerException;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.CMConstants.ChangeType;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.helix.messaging.DefaultMessagingService;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.tools.PropertiesReader;
import com.linkedin.helix.util.CMUtil;

public class DynamicFileClusterManager implements ClusterManager
{
  private static final Logger LOG = Logger
      .getLogger(StaticFileClusterManager.class.getName());
  private final FileBasedDataAccessor _fileDataAccessor;

  private final String _clusterName;
  private final InstanceType _instanceType;
  private final String _instanceName;
  private boolean _isConnected;
  private final List<CallbackHandlerForFile> _handlers;
  private final FileClusterManagementTool _mgmtTool;

  public static final String _sessionId = "12345";
  public static final String configFile = "configFile";
  private final DefaultMessagingService _messagingService;
  private final FilePropertyStore<ZNRecord> _store;
  private final String _version;

  public DynamicFileClusterManager(String clusterName,
                                   String instanceName,
                                   InstanceType instanceType,
                                   FilePropertyStore<ZNRecord> store)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _instanceType = instanceType;

    _handlers = new ArrayList<CallbackHandlerForFile>();

    _store = store;
    _fileDataAccessor = new FileBasedDataAccessor(_store, clusterName); // accessor;

    _mgmtTool = new FileClusterManagementTool(_store);
    _messagingService = new DefaultMessagingService(this);
    if (instanceType == InstanceType.PARTICIPANT)
    {
      addLiveInstance();
      addMessageListener(_messagingService.getExecutor(), _instanceName);
    }

//    _store.start();

    _version = new PropertiesReader("cluster-manager-version.properties")
                                .getProperty("clustermanager.version");
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
    final String path = CMUtil.getIdealStatePath(_clusterName);

    CallbackHandlerForFile callbackHandler = createCallBackHandler(path,
        listener, new EventType[]
        { EventType.NodeDataChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, IDEAL_STATE);
    _handlers.add(callbackHandler);

  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener)
  {
    final String path = CMUtil.getLiveInstancesPath(_clusterName);
    CallbackHandlerForFile callbackHandler = createCallBackHandler(path,
        listener, new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, LIVE_INSTANCE);
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
    final String path = CMUtil.getMessagePath(_clusterName, instanceName);

    CallbackHandlerForFile callbackHandler = createCallBackHandler(path,
        listener, new EventType[]
        { EventType.NodeDataChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, ChangeType.MESSAGE);
    _handlers.add(callbackHandler);

  }

  @Override
  public void addCurrentStateChangeListener(
      CurrentStateChangeListener listener, String instanceName, String sessionId)
  {
    final String path = CMUtil.getCurrentStateBasePath(_clusterName,
        instanceName) + "/" + sessionId;

    CallbackHandlerForFile callbackHandler = createCallBackHandler(path,
        listener, new EventType[]
        { EventType.NodeChildrenChanged, EventType.NodeDeleted,
            EventType.NodeCreated }, CURRENT_STATE);
    _handlers.add(callbackHandler);
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener)
  {
    throw new UnsupportedOperationException(
        "addExternalViewChangeListener() is NOT supported by File Based cluster manager");
  }

  @Override
  public ClusterDataAccessor getDataAccessor()
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

  private void addLiveInstance()
  {
    LiveInstance liveInstance = new LiveInstance(_instanceName);
    liveInstance.setSessionId(_sessionId);
    _fileDataAccessor.setProperty(PropertyType.LIVEINSTANCES, liveInstance.getRecord(), _instanceName);
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

  private CallbackHandlerForFile createCallBackHandler(String path,
      Object listener, EventType[] eventTypes, ChangeType changeType)
  {
    if (listener == null)
    {
      throw new ClusterManagerException("Listener cannot be null");
    }
    return new CallbackHandlerForFile(this, path, listener, eventTypes,
        changeType);
  }

  @Override
  public ClusterManagementService getClusterManagmentTool()
  {
    return _mgmtTool;
  }

  @Override
  public PropertyStore<ZNRecord> getPropertyStore()
  {
    // TODO Auto-generated method stub
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
public void addHealthStateChangeListener(HealthStateChangeListener listener,
		String instanceName) throws Exception {
	// TODO Auto-generated method stub

}

  @Override
  public String getVersion()
  {
    return _version;
  }

}
