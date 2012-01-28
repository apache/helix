package com.linkedin.clustermanager.agent.zk;

import static com.linkedin.clustermanager.CMConstants.ChangeType.CONFIG;
import static com.linkedin.clustermanager.CMConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.clustermanager.CMConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.MESSAGE;
import static com.linkedin.clustermanager.CMConstants.ChangeType.MESSAGES_CONTROLLER;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.HealthStateChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.HealthStat;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
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
    synchronized (_listener)
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
        List<IdealState> idealStates = _accessor.getChildValues(IdealState.class, PropertyType.IDEALSTATES);
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      }
      else if (_changeType == CONFIG)
      {

        ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<InstanceConfig> configs = _accessor.getChildValues(InstanceConfig.class, PropertyType.CONFIGS);
        configChangeListener.onConfigChange(configs, changeContext);

      }
      else if (_changeType == LIVE_INSTANCE)
      {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        subscribeForChanges(changeContext, true, false);
        List<LiveInstance> liveInstances = _accessor.getChildValues(LiveInstance.class, PropertyType.LIVEINSTANCES);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      }
      else if (_changeType == CURRENT_STATE)
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        String[] pathParts = _path.split("/");
        List<CurrentState> currentStates = _accessor.getChildValues(CurrentState.class,
                                                                    PropertyType.CURRENTSTATES,
                                                                    instanceName,
                                                                    pathParts[pathParts.length - 1]);

        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext);

      }
      else if (_changeType == MESSAGE)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, true, false);
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        List<Message> messages =  _accessor.getChildValues(Message.class, PropertyType.MESSAGES, instanceName);

//        // TODO remove this, enforce a zk manager to have an id always
//        if (instanceName == null)
//        {
//          logger.error("Instance does NOT have a name; use CONTROLLER");
//          instanceName = "CONTROLLER";
//        }
        messageListener.onMessage(instanceName, messages, changeContext);

      }
      else if (_changeType == MESSAGES_CONTROLLER)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, true, false);
        List<Message> messages =  _accessor.getChildValues(Message.class, PropertyType.MESSAGES_CONTROLLER);

        // TODO enforce a zk manager to have an id
        messageListener.onMessage(null, messages, changeContext);

      }
      else if (_changeType == EXTERNAL_VIEW)
      {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<ExternalView> externalViewList = _accessor.getChildValues(ExternalView.class,
                                                                       PropertyType.EXTERNALVIEW);

        externalViewListener.onExternalViewChange(externalViewList, changeContext);
      }
      else if (_changeType == ChangeType.CONTROLLER)
      {
        ControllerChangeListener controllerChangelistener = (ControllerChangeListener) _listener;
        subscribeForChanges(changeContext, true, false);
        controllerChangelistener.onControllerChange(changeContext);
      }
      else if (_changeType == ChangeType.HEALTH)
      {
        HealthStateChangeListener healthStateChangeListener = (HealthStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true); // TODO: figure out settings here
        String instanceName = CMUtil.getInstanceNameFromPath(_path);
        List<HealthStat> healthReportList = _accessor.getChildValues(HealthStat.class,
                                                                     PropertyType.HEALTHREPORT,
                                                                     instanceName);
        // List<ZNRecord> reports = ZKUtil.getChildren(_zkClient, _path);
        healthStateChangeListener.onHealthChange(instanceName,
                                                 healthReportList,
                                                 changeContext);
      }

      if (logger.isDebugEnabled())
      {
        logger.debug(Thread.currentThread().getId() + " END:INVOKE "
            + _path + " listener:" + _listener.getClass().getCanonicalName());
      }
    }
  }

  private void subscribeForChanges(NotificationContext changeContext,
      boolean watchParent, boolean watchChild)
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
      logger.info("can't subscribe for data change on childs, path:" + _path
          + " doesn't exist");
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
