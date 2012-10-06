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
package com.linkedin.helix.manager.file;

import static com.linkedin.helix.HelixConstants.ChangeType.CONFIG;
import static com.linkedin.helix.HelixConstants.ChangeType.CURRENT_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.EXTERNAL_VIEW;
import static com.linkedin.helix.HelixConstants.ChangeType.IDEAL_STATE;
import static com.linkedin.helix.HelixConstants.ChangeType.LIVE_INSTANCE;
import static com.linkedin.helix.HelixConstants.ChangeType.MESSAGE;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.store.PropertyChangeListener;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.util.HelixUtil;

// TODO remove code duplication: CallbackHandler and CallbackHandlerForFile
@Deprecated
public class FileCallbackHandler implements PropertyChangeListener<ZNRecord>
{

  private static Logger LOG = Logger.getLogger(FileCallbackHandler.class);

  private final String _path;
  private final Object _listener;
  private final EventType[] _eventTypes;
  private final ChangeType _changeType;
//  private final FileDataAccessor _accessor;
  private final FileHelixDataAccessor _accessor;
  private final AtomicLong lastNotificationTimeStamp;
  private final HelixManager _manager;
  private final FilePropertyStore<ZNRecord> _store;

  public FileCallbackHandler(HelixManager manager, String path, Object listener,
      EventType[] eventTypes, ChangeType changeType)
  {
    this._manager = manager;
    this._accessor = (FileHelixDataAccessor) manager.getHelixDataAccessor();
    this._path = path;
    this._listener = listener;
    this._eventTypes = eventTypes;
    this._changeType = changeType;
    this._store = (FilePropertyStore<ZNRecord>) _accessor.getStore();
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
      if (LOG.isDebugEnabled())
      {
        LOG.debug(Thread.currentThread().getId() + " START:INVOKE "
            + changeContext.getPathChanged() + " listener:"
            + _listener.getClass().getCanonicalName());
      }
      
      Builder keyBuilder = _accessor.keyBuilder();
      
      if (_changeType == IDEAL_STATE)
      {
        // System.err.println("ideal state change");
        IdealStateChangeListener idealStateChangeListener = (IdealStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<IdealState> idealStates = _accessor.getChildValues(keyBuilder.idealStates());
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      } else if (_changeType == CONFIG)
      {

        ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<InstanceConfig> configs = _accessor.getChildValues(keyBuilder.instanceConfigs());
        configChangeListener.onConfigChange(configs, changeContext);

      } else if (_changeType == LIVE_INSTANCE)
      {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        subscribeForChanges(changeContext, true, false);
        List<LiveInstance> liveInstances = _accessor.getChildValues(keyBuilder.liveInstances());
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      } else if (_changeType == CURRENT_STATE)
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        String instanceName = HelixUtil.getInstanceNameFromPath(_path);
        String[] pathParts = _path.split("/");
        List<CurrentState> currentStates = _accessor.getChildValues(keyBuilder.currentStates(instanceName, pathParts[pathParts.length - 1]));
        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext);

      } else if (_changeType == MESSAGE)
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, true, false);
        String instanceName = _manager.getInstanceName();
        List<Message> messages = _accessor.getChildValues(keyBuilder.messages(instanceName));
        messageListener.onMessage(instanceName, messages, changeContext);
      } else if (_changeType == EXTERNAL_VIEW)
      {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        subscribeForChanges(changeContext, true, true);
        List<ExternalView> externalViewList = _accessor.getChildValues(keyBuilder.externalViews());
        externalViewListener.onExternalViewChange(externalViewList, changeContext);
      } else if (_changeType == ChangeType.CONTROLLER)
      {
        ControllerChangeListener controllerChangelistener = (ControllerChangeListener) _listener;
        subscribeForChanges(changeContext, true, false);
        controllerChangelistener.onControllerChange(changeContext);
      }

      if (LOG.isDebugEnabled())
      {
        LOG.debug(Thread.currentThread().getId() + " END:INVOKE " + changeContext.getPathChanged()
            + " listener:" + _listener.getClass().getCanonicalName());
      }
    }
  }

  private void subscribeForChanges(NotificationContext changeContext, boolean watchParent,
      boolean watchChild)
  {
    if (changeContext.getType() == NotificationContext.Type.INIT)
    {
      try
      {
        // _accessor.subscribeForPropertyChange(_path, this);
        _store.subscribeForPropertyChange(_path, this);
      } catch (PropertyStoreException e)
      {
        LOG.error("fail to subscribe for changes" + "\nexception:" + e);
      }
    }
  }

  public EventType[] getEventTypes()
  {
    return _eventTypes;
  }

  // this will invoke the listener so that it sets up the initial values from
  // the file property store if any exists
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
      // TODO handle exception
      LOG.error("fail to init", e);
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
      // TODO handle exception
      LOG.error("fail to reset" + "\nexception:" + e);
      // ZKExceptionHandler.getInstance().handle(e);
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

  @Override
  public void onPropertyChange(String key)
  {
    // debug
    // LOG.error("on property change, key:" + key + ", path:" + _path);

    try
    {
      if (needToNotify(key))
      {
        // debug
        // System.err.println("notified on property change, key:" + key +
        // ", path:" +
        // path);

        updateNotificationTime(System.nanoTime());
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        invoke(changeContext);
      }
    } catch (Exception e)
    {
      // TODO handle exception
      // ZKExceptionHandler.getInstance().handle(e);
      LOG.error("fail onPropertyChange", e);
    }
  }

  private boolean needToNotify(String key)
  {
    boolean ret = false;
    switch (_changeType)
    {
    // both child/data changes matter
    case IDEAL_STATE:
    case CURRENT_STATE:
    case CONFIG:
      ret = key.startsWith(_path);
      break;
    // only child changes matter
    case LIVE_INSTANCE:
    case MESSAGE:
    case EXTERNAL_VIEW:
    case CONTROLLER:
      // ret = key.equals(_path);
      ret = key.startsWith(_path);
      break;
    default:
      break;
    }

    return ret;
  }
}
