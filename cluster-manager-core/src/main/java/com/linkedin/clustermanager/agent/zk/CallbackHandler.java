package com.linkedin.clustermanager.agent.zk;

import static com.linkedin.clustermanager.CMConstants.ChangeType.CONFIG;
import static com.linkedin.clustermanager.CMConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.clustermanager.CMConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.MESSAGE;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

public class CallbackHandler implements IZkChildListener, IZkDataListener

{

  private static Logger logger = Logger.getLogger(CallbackHandler.class);

  private final String _path;
  private final Object _listener;
  private final EventType[] _eventTypes;
  private final ClusterDataAccessor _accessor;
  private final ChangeType _changeType;
  private final ZkClient _zkClient;
  private final AtomicLong lastNotificationTimeStamp;
  private final ClusterManager _manager;

  public CallbackHandler(ClusterManager manager, ZkClient client, String path,
      Object listener, EventType[] eventTypes, ChangeType changeType)
  {
    this._manager = manager;
    this._accessor = manager.getDataAccessor();
    this._zkClient = client;
    this._path = path;
    this._listener = listener;
    this._eventTypes = eventTypes;
    this._changeType = changeType;
    lastNotificationTimeStamp = new AtomicLong(System.nanoTime());
    init();
  }

  public Object getPath()
  {
    return _path;
  }

  public void invoke(NotificationContext changeContext) throws Exception
  {
    // This allows the listener to work with one change at a time
    synchronized (_listener)
    {
      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " START:INVOKE "
            + changeContext.getPathChanged() + " listener:"
            + _listener.getClass().getCanonicalName());
      }
      if (_changeType == IDEAL_STATE)
      {

        IdealStateChangeListener idealStateChangeListener = (IdealStateChangeListener) _listener;
        subscribeForChanges(_path, true, true);
        List<ZNRecord> idealStates = _accessor
            .getClusterPropertyList(ClusterPropertyType.IDEALSTATES);
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      } else if (_changeType == CONFIG)
      {

        ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
        subscribeForChanges(_path, true, true);
        List<ZNRecord> configs = _accessor
            .getClusterPropertyList(ClusterPropertyType.CONFIGS);
        configChangeListener.onConfigChange(configs, changeContext);

      } else if (_changeType == LIVE_INSTANCE)
      {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        subscribeForChanges(_path, true, false);
        List<ZNRecord> liveInstances = _accessor
            .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances,
            changeContext);

      } else if (_changeType == CURRENT_STATE)
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(_path, true, true);
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        List<ZNRecord> currentStates = _accessor.getInstancePropertyList(
            instanceName, InstancePropertyType.CURRENTSTATES);
        currentStateChangeListener.onStateChange(instanceName, currentStates,
            changeContext);

      } else if (_changeType == MESSAGE)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(_path, true, false);
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        List<ZNRecord> messages = _accessor.getInstancePropertyList(
            instanceName, InstancePropertyType.MESSAGES);
        messageListener.onMessage(instanceName, messages, changeContext);

      } else if (_changeType == EXTERNAL_VIEW)
      {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        subscribeForChanges(_path, true, true);
        List<ZNRecord> externalViewList = _accessor
            .getClusterPropertyList(ClusterPropertyType.EXTERNALVIEW);
        externalViewListener.onExternalViewChange(externalViewList,
            changeContext);
      }
      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " END:INVOKE "
            + changeContext.getPathChanged() + " listener:"
            + _listener.getClass().getCanonicalName());
      }
    }
  }

  private void subscribeForChanges(String path, boolean watchParent,
      boolean watchChild)
  {
    // parent watch will be set by zkClient
    List<String> children = _zkClient.getChildren(path);
    for (String child : children)
    {
      String childPath = path + "/" + child;
      if (watchChild)
      {
        // its ok to subscribe changes multiple times since zkclient
        // checks the existence
        _zkClient.subscribeDataChanges(childPath, this);
      }
    }
  }

  public EventType[] getEventTypes()
  {
    return _eventTypes;
  }

  // this will invoke the listener so that it sets up the initial values from
  // the zookeeper if any exists
  public void init()
  {
    updateNotificationTime(System.nanoTime());
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.INIT);
      invoke(changeContext);
    } catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    updateNotificationTime(System.nanoTime());
    if (dataPath != null && dataPath.startsWith(_path))
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      invoke(changeContext);
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    updateNotificationTime(System.nanoTime());
    if (dataPath != null && dataPath.startsWith(_path))
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      _zkClient.unsubscribeChildChanges(dataPath, this);
      invoke(changeContext);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
      throws Exception
  {
    updateNotificationTime(System.nanoTime());
    if (parentPath != null && parentPath.startsWith(_path))
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      invoke(changeContext);
    }
  }

  private void updateNotificationTime(long nanoTime)
  {
    long l = lastNotificationTimeStamp.get();
    while (nanoTime > l)
    {
      boolean b = lastNotificationTimeStamp.compareAndSet(l, nanoTime);
      if (b)
      {
        break;
      } else
      {
        l = lastNotificationTimeStamp.get();
      }
    }
  }

}
