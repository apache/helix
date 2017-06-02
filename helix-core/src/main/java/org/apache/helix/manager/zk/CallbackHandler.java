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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.HelixCallbackMonitor;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import static org.apache.helix.HelixConstants.ChangeType.*;

public class CallbackHandler implements IZkChildListener, IZkDataListener {
  private static Logger logger = Logger.getLogger(CallbackHandler.class);

  /**
   * define the next possible notification types
   */
  private static Map<Type, List<Type>> nextNotificationType = new HashMap<>();
  static {
    nextNotificationType.put(Type.INIT, Arrays.asList(Type.CALLBACK, Type.FINALIZE));
    nextNotificationType.put(Type.CALLBACK, Arrays.asList(Type.CALLBACK, Type.FINALIZE));
    nextNotificationType.put(Type.FINALIZE, Arrays.asList(Type.INIT));
  }

  private final String _path;
  private final Object _listener;
  private final Set<EventType> _eventTypes;
  private final HelixDataAccessor _accessor;
  private final ChangeType _changeType;
  private final ZkClient _zkClient;
  private final AtomicLong _lastNotificationTimeStamp;
  private final HelixManager _manager;
  private final PropertyKey _propertyKey;
  BlockingQueue<NotificationContext> _queue = new LinkedBlockingQueue<>(1000);
  private boolean _batchModeEnabled = false;
  private boolean _preFetchEnabled = true;
  private HelixCallbackMonitor _monitor;

  /**
   * maintain the expected notification types
   * this is fix for HELIX-195: race condition between FINALIZE callbacks and Zk callbacks
   */
  private List<NotificationContext.Type> _expectTypes = nextNotificationType.get(Type.FINALIZE);

  public CallbackHandler(HelixManager manager, ZkClient client, PropertyKey propertyKey,
      Object listener, EventType[] eventTypes, ChangeType changeType) {
    this(manager, client, propertyKey, listener, eventTypes, changeType, null);
  }

  public CallbackHandler(HelixManager manager, ZkClient client, PropertyKey propertyKey,
      Object listener, EventType[] eventTypes, ChangeType changeType, HelixCallbackMonitor monitor) {
    if (listener == null) {
      throw new HelixException("listener could not be null");
    }

    this._manager = manager;
    this._accessor = manager.getHelixDataAccessor();
    this._zkClient = client;
    this._propertyKey = propertyKey;
    this._path = propertyKey.getPath();
    this._listener = listener;
    this._eventTypes = new HashSet<>(Arrays.asList(eventTypes));
    this._changeType = changeType;
    this._lastNotificationTimeStamp = new AtomicLong(System.nanoTime());
    this._queue = new LinkedBlockingQueue<>(1000);
    this._monitor = monitor;

    parseListenerProperties();

    logger.info("isAsyncBatchModeEnabled: " + _batchModeEnabled);
    logger.info("isPreFetchEnabled: " + _preFetchEnabled);

    if (_batchModeEnabled) {
      new Thread(new CallbackInvoker(this)).start();
    }
    init();
  }


  private void parseListenerProperties() {
    BatchMode batchMode = _listener.getClass().getAnnotation(BatchMode.class);
    PreFetch preFetch = _listener.getClass().getAnnotation(PreFetch.class);

    if (batchMode != null) {
      _batchModeEnabled = batchMode.enabled();
    }
    if (preFetch != null) {
      _preFetchEnabled = preFetch.enabled();
    }

    Class listenerClass = null;
    switch (_changeType) {
      case IDEAL_STATE:
        listenerClass = IdealStateChangeListener.class;
        break;
      case INSTANCE_CONFIG:
        if (_listener instanceof ConfigChangeListener) {
          listenerClass = ConfigChangeListener.class;
        } else if (_listener instanceof InstanceConfigChangeListener) {
          listenerClass = InstanceConfigChangeListener.class;
        }
        break;
      case CLUSTER_CONFIG:
        listenerClass = ClusterConfigChangeListener.class;
        break;
      case RESOURCE_CONFIG:
        listenerClass = ResourceConfigChangeListener.class;
        break;
      case CONFIG:
        listenerClass = ConfigChangeListener.class;
        break;
      case LIVE_INSTANCE:
        listenerClass = LiveInstanceChangeListener.class;
        break;
      case CURRENT_STATE:
        listenerClass = CurrentStateChangeListener.class;        ;
        break;
      case MESSAGE:
      case MESSAGES_CONTROLLER:
        listenerClass = MessageListener.class;
        break;
      case EXTERNAL_VIEW:
        listenerClass = ExternalViewChangeListener.class;
        break;
      case CONTROLLER:
        listenerClass = ControllerChangeListener.class;
    }

    Method callbackMethod = listenerClass.getMethods()[0];
    try {
      Method method = _listener.getClass()
          .getMethod(callbackMethod.getName(), callbackMethod.getParameterTypes());
      BatchMode batchModeInMethod = method.getAnnotation(BatchMode.class);
      PreFetch preFetchInMethod = method.getAnnotation(PreFetch.class);
      if (batchModeInMethod != null) {
        _batchModeEnabled = batchModeInMethod.enabled();
      }
      if (preFetchInMethod != null) {
        _preFetchEnabled = preFetchInMethod.enabled();
      }
    } catch (NoSuchMethodException e) {
      logger.warn(
          "No method " + callbackMethod.getName() + " defined in listener " + _listener.getClass()
              .getCanonicalName());
    }
  }


  public Object getListener() {
    return _listener;
  }

  public String getPath() {
    return _path;
  }

  class CallbackInvoker implements Runnable {
    private CallbackHandler handler;

    CallbackInvoker(CallbackHandler handler) {
      this.handler = handler;
    }

    public void run() {
      while (true) {
        try {
          NotificationContext notificationToProcess = _queue.take();
          int mergedCallbacks = 0;
          // remove all elements in the queue that have the same type
          while (true) {
            NotificationContext nextItem = _queue.peek();
            if (nextItem != null && notificationToProcess.getType() == nextItem.getType()) {
              notificationToProcess = _queue.take();
              mergedCallbacks++;
            } else {
              break;
            }
          }
          try {
            logger.info(
                "Num callbacks merged for path:" + handler.getPath() + " : " + mergedCallbacks);
            handler.invoke(notificationToProcess);
          } catch (Exception e) {
            logger.warn("Exception in callback processing thread. Skipping callback", e);
          }
        } catch (InterruptedException e) {
          logger.warn(
              "Interrupted exception in callback processing thread. Exiting thread, new callbacks will not be processed",
              e);
          break;
        }
      }
    }
  }

  public void enqueueTask(NotificationContext changeContext)
      throws Exception {
    //async mode only applicable to CALLBACK from ZK, During INIT and FINALIZE invoke the callback's immediately.
    if (_batchModeEnabled && changeContext.getType() == NotificationContext.Type.CALLBACK) {
      logger.info("Enqueuing callback");
      _queue.put(changeContext);
    } else {
      invoke(changeContext);
    }
    if (_monitor != null) {
      _monitor.increaseCallbackUnbatchedCounters(_changeType);
    }
  }

  public void invoke(NotificationContext changeContext) throws Exception {
    // This allows the listener to work with one change at a time
    synchronized (_manager) {
      Type type = changeContext.getType();
      if (!_expectTypes.contains(type)) {
        logger.warn("Skip processing callbacks for listener: " + _listener + ", path: " + _path
            + ", expected types: " + _expectTypes + " but was " + type);

        return;
      }
      _expectTypes = nextNotificationType.get(type);

      // Builder keyBuilder = _accessor.keyBuilder();
      long start = System.currentTimeMillis();
      if (logger.isInfoEnabled()) {
        logger.info(
            Thread.currentThread().getId() + " START:INVOKE " + _path + " listener:" + _listener
                .getClass().getCanonicalName());
      }

      if (_changeType == IDEAL_STATE) {
        IdealStateChangeListener idealStateChangeListener = (IdealStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true);
        List<IdealState> idealStates = preFetch(_propertyKey);
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);

      } else if (_changeType == ChangeType.INSTANCE_CONFIG) {
        subscribeForChanges(changeContext, _path, true);
        if (_listener instanceof ConfigChangeListener) {
          ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
          List<InstanceConfig> configs = preFetch(_propertyKey);
          configChangeListener.onConfigChange(configs, changeContext);
        } else if (_listener instanceof InstanceConfigChangeListener) {
          InstanceConfigChangeListener listener = (InstanceConfigChangeListener) _listener;
          List<InstanceConfig> configs = preFetch(_propertyKey);
          listener.onInstanceConfigChange(configs, changeContext);
        }
      } else if (_changeType == ChangeType.RESOURCE_CONFIG) {
        subscribeForChanges(changeContext, _path, true);
        ResourceConfigChangeListener listener = (ResourceConfigChangeListener) _listener;
        List<ResourceConfig> configs = preFetch(_propertyKey);
        listener.onResourceConfigChange(configs, changeContext);

      } else if (_changeType == ChangeType.CLUSTER_CONFIG) {
        subscribeForChanges(changeContext, _path, true);
        ClusterConfigChangeListener listener = (ClusterConfigChangeListener) _listener;
        ClusterConfig config = null;
        if (_preFetchEnabled) {
          config = _accessor.getProperty(_propertyKey);
        }
        listener.onClusterConfigChange(config, changeContext);

      } else if (_changeType == CONFIG) {
        subscribeForChanges(changeContext, _path, true);
        ScopedConfigChangeListener listener = (ScopedConfigChangeListener) _listener;
        List<HelixProperty> configs = preFetch(_propertyKey);
        listener.onConfigChange(configs, changeContext);

      } else if (_changeType == LIVE_INSTANCE) {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true);
        List<LiveInstance> liveInstances = preFetch(_propertyKey);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      } else if (_changeType == CURRENT_STATE) {
        CurrentStateChangeListener currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true);
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<CurrentState> currentStates = preFetch(_propertyKey);
        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext);

      } else if (_changeType == MESSAGE) {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, _path, false);
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<Message> messages = preFetch(_propertyKey);
        messageListener.onMessage(instanceName, messages, changeContext);

      } else if (_changeType == MESSAGES_CONTROLLER) {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, _path, false);
        List<Message> messages = preFetch(_propertyKey);
        messageListener.onMessage(_manager.getInstanceName(), messages, changeContext);

      } else if (_changeType == EXTERNAL_VIEW) {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true);
        List<ExternalView> externalViewList = preFetch(_propertyKey);
        externalViewListener.onExternalViewChange(externalViewList, changeContext);

      } else if (_changeType == ChangeType.CONTROLLER) {
        ControllerChangeListener controllerChangelistener = (ControllerChangeListener) _listener;
        subscribeForChanges(changeContext, _path, false);
        controllerChangelistener.onControllerChange(changeContext);
      }

      long end = System.currentTimeMillis();
      if (logger.isInfoEnabled()) {
        logger.info(
            Thread.currentThread().getId() + " END:INVOKE " + _path + " listener:" + _listener
                .getClass().getCanonicalName() + " Took: " + (end - start) + "ms");
      }

      if (_monitor != null) {
        _monitor.increaseCallbackCounters(_changeType, end - start);
      }
    }
  }

  private <T extends HelixProperty> List<T> preFetch(PropertyKey key) {
    if (_preFetchEnabled) {
      return _accessor.getChildValues(key);
    } else {
      return Collections.emptyList();
    }
  }

  private void subscribeChildChange(String path, NotificationContext context) {
    NotificationContext.Type type = context.getType();
    if (type == NotificationContext.Type.INIT || type == NotificationContext.Type.CALLBACK) {
      logger.info(
          _manager.getInstanceName() + " subscribes child-change. path: " + path + ", listener: "
              + _listener);
      _zkClient.subscribeChildChanges(path, this);
    } else if (type == NotificationContext.Type.FINALIZE) {
      logger.info(
          _manager.getInstanceName() + " unsubscribe child-change. path: " + path + ", listener: "
              + _listener);

      _zkClient.unsubscribeChildChanges(path, this);
    }
  }

  private void subscribeDataChange(String path, NotificationContext context) {
    NotificationContext.Type type = context.getType();
    if (type == NotificationContext.Type.INIT || type == NotificationContext.Type.CALLBACK) {
      logger.info(
          _manager.getInstanceName() + " subscribe data-change. path: " + path + ", listener: "
              + _listener);
      _zkClient.subscribeDataChanges(path, this);

    } else if (type == NotificationContext.Type.FINALIZE) {
      logger.info(
          _manager.getInstanceName() + " unsubscribe data-change. path: " + path + ", listener: "
              + _listener);

      _zkClient.unsubscribeDataChanges(path, this);
    }
  }

  // TODO watchParent is always true. consider remove it
  private void subscribeForChanges(NotificationContext context, String path, boolean watchChild) {
    long start = System.currentTimeMillis();

    if (_eventTypes.contains(EventType.NodeDataChanged) || _eventTypes
        .contains(EventType.NodeCreated) || _eventTypes.contains(EventType.NodeDeleted)) {
      logger.debug("Subscribing data change listener to path:" + path);
      subscribeDataChange(path, context);
    }

    if (_eventTypes.contains(EventType.NodeChildrenChanged)) {
      logger.debug("Subscribing child change listener to path:" + path);
      subscribeChildChange(path, context);
      if (watchChild) {
        logger.debug("Subscribing data change listener to all children for path:" + path);
        try {
          switch (_changeType) {
          case CURRENT_STATE:
          case IDEAL_STATE:
          case EXTERNAL_VIEW: {
            // check if bucketized
            BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
            List<ZNRecord> records = baseAccessor.getChildren(path, null, 0);
            for (ZNRecord record : records) {
              HelixProperty property = new HelixProperty(record);
              String childPath = path + "/" + record.getId();

              int bucketSize = property.getBucketSize();
              if (bucketSize > 0) {
                // subscribe both data-change and child-change on bucketized parent node
                // data-change gives a delete-callback which is used to remove watch
                subscribeChildChange(childPath, context);
                subscribeDataChange(childPath, context);

                // subscribe data-change on bucketized child
                List<String> bucketizedChildNames = _zkClient.getChildren(childPath);
                if (bucketizedChildNames != null) {
                  for (String bucketizedChildName : bucketizedChildNames) {
                    String bucketizedChildPath = childPath + "/" + bucketizedChildName;
                    subscribeDataChange(bucketizedChildPath, context);
                  }
                }
              } else {
                subscribeDataChange(childPath, context);
              }
            }
            break;
          }
          default: {
            List<String> childNames = _zkClient.getChildren(path);
            if (childNames != null) {
              for (String childName : childNames) {
                String childPath = path + "/" + childName;
                subscribeDataChange(childPath, context);
              }
            }
            break;
          }
          }
        } catch (ZkNoNodeException e) {
          logger.warn(
              "fail to subscribe child/data change. path: " + path + ", listener: " + _listener, e);
        }
      }
    }

    long end = System.currentTimeMillis();
    logger.info("Subcribing to path:" + path + " took:" + (end - start));
  }

  public EventType[] getEventTypes() {
    return (EventType[]) _eventTypes.toArray();
  }

  /**
   * Invoke the listener so that it sets up the initial values from the zookeeper if any
   * exists
   */
  public void init() {
    updateNotificationTime(System.nanoTime());
    try {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.INIT);
      enqueueTask(changeContext);
    } catch (Exception e) {
      String msg = "Exception while invoking init callback for listener:" + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data) {
    try {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path)) {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        enqueueTask(changeContext);
      }
    } catch (Exception e) {
      String msg = "exception in handling data-change. path: " + dataPath + ", listener: " + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override public void handleDataDeleted(String dataPath) {
    try {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path)) {
        logger.info(_manager.getInstanceName() + " unsubscribe data-change. path: " + dataPath
            + ", listener: " + _listener);
        _zkClient.unsubscribeDataChanges(dataPath, this);

        // only needed for bucketized parent, but OK if we don't have child-change
        // watch on the bucketized parent path
        logger.info(_manager.getInstanceName() + " unsubscribe child-change. path: " + dataPath
            + ", listener: " + _listener);
        _zkClient.unsubscribeChildChanges(dataPath, this);
        // No need to invoke() since this event will handled by child-change on parent-node
        // NotificationContext changeContext = new NotificationContext(_manager);
        // changeContext.setType(NotificationContext.Type.CALLBACK);
        // invoke(changeContext);
      }
    } catch (Exception e) {
      String msg = "exception in handling data-delete-change. path: " + dataPath + ", listener: "
          + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) {
    try {
      updateNotificationTime(System.nanoTime());
      if (parentPath != null && parentPath.startsWith(_path)) {
        NotificationContext changeContext = new NotificationContext(_manager);

        if (currentChilds == null && parentPath.equals(_path)) {
          // _path has been removed, remove this listener
          // removeListener will call handler.reset(), which in turn call invoke() on FINALIZE type
          _manager.removeListener(_propertyKey, _listener);
        } else {
          changeContext.setType(NotificationContext.Type.CALLBACK);
          enqueueTask(changeContext);
        }
      }
    } catch (Exception e) {
      String msg = "exception in handling child-change. instance: " + _manager.getInstanceName()
          + ", parentPath: " + parentPath + ", listener: " + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  /**
   * Invoke the listener for the last time so that the listener could clean up resources
   */
  public void reset() {
    try {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.FINALIZE);
      enqueueTask(changeContext);
    } catch (Exception e) {
      String msg = "Exception while resetting the listener:" + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  private void updateNotificationTime(long nanoTime) {
    long l = _lastNotificationTimeStamp.get();
    while (nanoTime > l) {
      boolean b = _lastNotificationTimeStamp.compareAndSet(l, nanoTime);
      if (b) {
        break;
      } else {
        l = _lastNotificationTimeStamp.get();
      }
    }
  }

}
