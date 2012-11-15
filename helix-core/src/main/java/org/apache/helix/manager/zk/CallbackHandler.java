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

import static org.apache.helix.HelixConstants.ChangeType.CONFIG;
import static org.apache.helix.HelixConstants.ChangeType.CURRENT_STATE;
import static org.apache.helix.HelixConstants.ChangeType.EXTERNAL_VIEW;
import static org.apache.helix.HelixConstants.ChangeType.IDEAL_STATE;
import static org.apache.helix.HelixConstants.ChangeType.LIVE_INSTANCE;
import static org.apache.helix.HelixConstants.ChangeType.MESSAGE;
import static org.apache.helix.HelixConstants.ChangeType.MESSAGES_CONTROLLER;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HealthStateChangeListener;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

public class CallbackHandler implements IZkChildListener, IZkDataListener

{

  private static Logger           logger = Logger.getLogger(CallbackHandler.class);

  private final String            _path;
  private final Object            _listener;
  private final EventType[]       _eventTypes;
  private final HelixDataAccessor _accessor;
  private final ChangeType        _changeType;
  private final ZkClient          _zkClient;
  private final AtomicLong        lastNotificationTimeStamp;
  private final HelixManager      _manager;

  public CallbackHandler(HelixManager manager,
                         ZkClient client,
                         String path,
                         Object listener,
                         EventType[] eventTypes,
                         ChangeType changeType)
  {
    this._manager = manager;
    this._accessor = manager.getHelixDataAccessor();
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

  public String getPath()
  {
    return _path;
  }

  public void invoke(NotificationContext changeContext) throws Exception
  {
    // This allows the listener to work with one change at a time
    synchronized (_manager)
    {
      Builder keyBuilder = _accessor.keyBuilder();
      long start = System.currentTimeMillis();
      if (logger.isInfoEnabled())
      {
        logger.info(Thread.currentThread().getId() + " START:INVOKE "
        // + changeContext.getPathChanged()
            + _path + " listener:" + _listener.getClass().getCanonicalName());
      }

      if (_changeType == IDEAL_STATE)
      {

        IdealStateChangeListener idealStateChangeListener =
            (IdealStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<IdealState> idealStates = _accessor.getChildValues(keyBuilder.idealStates());

        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      }
      else if (_changeType == CONFIG)
      {

        ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<InstanceConfig> configs =
            _accessor.getChildValues(keyBuilder.instanceConfigs());

        configChangeListener.onConfigChange(configs, changeContext);

      }
      else if (_changeType == LIVE_INSTANCE)
      {
        LiveInstanceChangeListener liveInstanceChangeListener =
            (LiveInstanceChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<LiveInstance> liveInstances =
            _accessor.getChildValues(keyBuilder.liveInstances());

        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      }
      else if (_changeType == CURRENT_STATE)
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        String[] pathParts = _path.split("/");

        // TODO: fix this
        List<CurrentState> currentStates =
            _accessor.getChildValues(keyBuilder.currentStates(instanceName,
                                                              pathParts[pathParts.length - 1]));

        currentStateChangeListener.onStateChange(instanceName,
                                                 currentStates,
                                                 changeContext);

      }
      else if (_changeType == MESSAGE)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, _path, true, false);
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<Message> messages =
            _accessor.getChildValues(keyBuilder.messages(instanceName));

        messageListener.onMessage(instanceName, messages, changeContext);

      }
      else if (_changeType == MESSAGES_CONTROLLER)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, _path, true, false);
        List<Message> messages =
            _accessor.getChildValues(keyBuilder.controllerMessages());

        messageListener.onMessage(_manager.getInstanceName(), messages, changeContext);

      }
      else if (_changeType == EXTERNAL_VIEW)
      {
        ExternalViewChangeListener externalViewListener =
            (ExternalViewChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<ExternalView> externalViewList =
            _accessor.getChildValues(keyBuilder.externalViews());

        externalViewListener.onExternalViewChange(externalViewList, changeContext);
      }
      else if (_changeType == ChangeType.CONTROLLER)
      {
        ControllerChangeListener controllerChangelistener =
            (ControllerChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, false);
        controllerChangelistener.onControllerChange(changeContext);
      }
      else if (_changeType == ChangeType.HEALTH)
      {
        HealthStateChangeListener healthStateChangeListener =
            (HealthStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true); // TODO: figure out
        // settings here
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);

        List<HealthStat> healthReportList =
            _accessor.getChildValues(keyBuilder.healthReports(instanceName));

        healthStateChangeListener.onHealthChange(instanceName,
                                                 healthReportList,
                                                 changeContext);
      }
      long end = System.currentTimeMillis();
      if (logger.isInfoEnabled())
      {
        logger.info(Thread.currentThread().getId() + " END:INVOKE " + _path
            + " listener:" + _listener.getClass().getCanonicalName() + " Took: "
            + (end - start));
      }
    }
  }

  private void subscribeForChanges(NotificationContext context,
                                   String path,
                                   boolean watchParent,
                                   boolean watchChild)
  {
    NotificationContext.Type type = context.getType();
    if (watchParent)
    {
      if (type == NotificationContext.Type.INIT
          || type == NotificationContext.Type.CALLBACK)
      {
        logger.info(_manager.getInstanceName() + " subscribe child change@" + path);
        _zkClient.subscribeChildChanges(path, this);
      }
      else if (watchParent && type == NotificationContext.Type.FINALIZE)
      {
        logger.info(_manager.getInstanceName() + " UNsubscribe child change@" + path);
        _zkClient.unsubscribeChildChanges(path, this);
      }
    }

    if (watchChild)
    {
      try
      {
        List<String> childNames = _zkClient.getChildren(path);
        if (childNames == null || childNames.size() == 0)
        {
          return;
        }

        for (String childName : childNames)
        {
          String childPath = path + "/" + childName;
          if (type == NotificationContext.Type.INIT
              || type == NotificationContext.Type.CALLBACK)
          {
            if (logger.isDebugEnabled())
            {
              logger.debug(_manager.getInstanceName() + " subscribe data change@" + childPath);
            }
            _zkClient.subscribeDataChanges(childPath, this);

          }
          else if (type == NotificationContext.Type.FINALIZE)
          {
            logger.info(_manager.getInstanceName() + " UNsubscribe data change@" + childPath);
            _zkClient.unsubscribeDataChanges(childPath, this);
          }

          subscribeForChanges(context, childPath, watchParent, watchChild);
        }
      }
      catch (ZkNoNodeException e)
      {
        logger.warn("fail to subscribe child data change@" + path);
      }
    }

  }

  public EventType[] getEventTypes()
  {
    return _eventTypes;
  }

  /**
   * Invoke the listener so that it sets up the initial values from the zookeeper if any
   * exists
   * 
   */
  public void init()
  {
    updateNotificationTime(System.nanoTime());
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.INIT);
      invoke(changeContext);
    }
    catch (Exception e)
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
    }
    catch (Exception e)
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
    }
    catch (Exception e)
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
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  /**
   * Invoke the listener for the last time so that the listener could clean up resources
   * 
   */
  public void reset()
  {
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.FINALIZE);
      invoke(changeContext);
    }
    catch (Exception e)
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
      }
      else
      {
        l = lastNotificationTimeStamp.get();
      }
    }
  }

}
