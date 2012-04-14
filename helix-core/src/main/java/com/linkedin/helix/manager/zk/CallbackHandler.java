package com.linkedin.helix.manager.zk;

import static com.linkedin.helix.HelixConstants.ChangeType.CONFIG;
import static com.linkedin.helix.HelixConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.helix.HelixConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.helix.HelixConstants.ChangeType.MESSAGE;
import static com.linkedin.helix.HelixConstants.ChangeType.MESSAGES_CONTROLLER;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.util.HelixUtil;

public class CallbackHandler implements IZkChildListener, IZkDataListener

{

  private static Logger logger = Logger.getLogger(CallbackHandler.class);

  private final String _path;
  private final Object _listener;
  private final EventType[] _eventTypes;
  private final DataAccessor _accessor;
  private final ChangeType _changeType;
  private final ZkClient _zkClient;
  private final AtomicLong lastNotificationTimeStamp;
  private final HelixManager _manager;

  public CallbackHandler(HelixManager manager, ZkClient client, String path, Object listener,
      EventType[] eventTypes, ChangeType changeType)
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

  public Object getListener()
  {
    return _listener;
  }

  public Object getPath()
  {
    return _path;
  }

  public void invoke(NotificationContext changeContext) throws Exception
  {
    // This allows the listener to work with one change at a time
    synchronized (_manager)
    {
      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " START:INVOKE "
        // + changeContext.getPathChanged()
            + _path + " listener:" + _listener.getClass().getCanonicalName());
      }

      if (_changeType == IDEAL_STATE)
      {

        IdealStateChangeListener idealStateChangeListener = (IdealStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<IdealState> idealStates = _accessor.getChildValues(IdealState.class,
            PropertyType.IDEALSTATES);
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      } else if (_changeType == CONFIG)
      {

        ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<InstanceConfig> configs = _accessor.getChildValues(InstanceConfig.class,
            PropertyType.CONFIGS, ConfigScopeProperty.PARTICIPANT.toString());
        configChangeListener.onConfigChange(configs, changeContext);

      } else if (_changeType == LIVE_INSTANCE)
      {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        subscribeForChanges(changeContext, true, false);
        List<LiveInstance> liveInstances = _accessor.getChildValues(LiveInstance.class,
            PropertyType.LIVEINSTANCES);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      } else if (_changeType == CURRENT_STATE)
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        String instanceName = HelixUtil.getInstanceNameFromPath(_path);
        String[] pathParts = _path.split("/");
        List<CurrentState> currentStates = _accessor.getChildValues(CurrentState.class,
            PropertyType.CURRENTSTATES, instanceName, pathParts[pathParts.length - 1]);

        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext);

      } else if (_changeType == MESSAGE)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, true, false);
        String instanceName = HelixUtil.getInstanceNameFromPath(_path);
        List<Message> messages = _accessor.getChildValues(Message.class, PropertyType.MESSAGES,
            instanceName);
        messageListener.onMessage(instanceName, messages, changeContext);

      } else if (_changeType == MESSAGES_CONTROLLER)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, true, false);
        List<Message> messages = _accessor.getChildValues(Message.class,
            PropertyType.MESSAGES_CONTROLLER);
        messageListener.onMessage(_manager.getInstanceName(), messages, changeContext);

      } else if (_changeType == EXTERNAL_VIEW)
      {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<ExternalView> externalViewList = _accessor.getChildValues(ExternalView.class,
            PropertyType.EXTERNALVIEW);

        externalViewListener.onExternalViewChange(externalViewList, changeContext);
      } else if (_changeType == ChangeType.CONTROLLER)
      {
        ControllerChangeListener controllerChangelistener = (ControllerChangeListener) _listener;
        subscribeForChanges(changeContext, true, false);
        controllerChangelistener.onControllerChange(changeContext);
      } else if (_changeType == ChangeType.HEALTH)
      {
        HealthStateChangeListener healthStateChangeListener = (HealthStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true); // TODO: figure out
                                                        // settings here
        String instanceName = HelixUtil.getInstanceNameFromPath(_path);
        List<HealthStat> healthReportList = _accessor.getChildValues(HealthStat.class,
            PropertyType.HEALTHREPORT, instanceName);
        // List<ZNRecord> reports = ZKUtil.getChildren(_zkClient, _path);
        healthStateChangeListener.onHealthChange(instanceName, healthReportList, changeContext);
      }

      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " END:INVOKE " + _path + " listener:"
            + _listener.getClass().getCanonicalName());
      }
    }
  }

  private void subscribeForChanges(NotificationContext changeContext, boolean watchParent,
      boolean watchChild)
  {
    // parent watch will be set by zkClient
    if (watchParent)
    {
      if (changeContext.getType() == NotificationContext.Type.INIT)
      {
        _zkClient.subscribeChildChanges(this._path, this);
      } else if (changeContext.getType() == NotificationContext.Type.FINALIZE)
      {
        _zkClient.unsubscribeChildChanges(this._path, this);
      }
    }

    if (_zkClient.exists(_path))
    {
      if (watchChild)
      {
        List<String> children = _zkClient.getChildren(_path);
        for (String child : children)
        {
          String childPath = _path + "/" + child;
          if (changeContext.getType() == NotificationContext.Type.INIT
              || changeContext.getType() == NotificationContext.Type.CALLBACK)
          {
            _zkClient.subscribeDataChanges(childPath, this);
          } else if (changeContext.getType() == NotificationContext.Type.FINALIZE)
          {
            _zkClient.unsubscribeDataChanges(childPath, this);
          }
        }
      }
    } else
    {
      logger.info("can't subscribe for data change on childs, path:" + _path + " doesn't exist");
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
  public void handleDataChange(String dataPath, Object data)
  {
    try
    {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path))
      {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        invoke(changeContext);
      }
    } catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleDataDeleted(String dataPath)
  {
    try
    {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path))
      {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        _zkClient.unsubscribeChildChanges(dataPath, this);
        invoke(changeContext);
      }
    } catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
  {
    try
    {
      updateNotificationTime(System.nanoTime());
      if (parentPath != null && parentPath.startsWith(_path))
      {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        invoke(changeContext);
      }
    } catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  public void reset()
  {
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.FINALIZE);
      invoke(changeContext);
    } catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
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
