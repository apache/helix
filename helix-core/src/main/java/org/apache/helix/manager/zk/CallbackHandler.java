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
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.HelixCallbackMonitor;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.HelixConstants.ChangeType.*;

@PreFetch (enabled = false)
public class CallbackHandler implements IZkChildListener, IZkDataListener {
  private static Logger logger = LoggerFactory.getLogger(CallbackHandler.class);

  /**
   * define the next possible notification types
   */
  private static Map<Type, List<Type>> nextNotificationType = new HashMap<>();
  static {
    nextNotificationType.put(Type.INIT, Arrays.asList(Type.CALLBACK, Type.FINALIZE));
    nextNotificationType.put(Type.CALLBACK, Arrays.asList(Type.CALLBACK, Type.FINALIZE));
    nextNotificationType.put(Type.FINALIZE, Arrays.asList(Type.INIT));
  }

  // processor to handle async zk event resubscription.
  private static DedupEventProcessor SubscribeChangeEventProcessor;

  private final String _path;
  private final Object _listener;
  private final Set<EventType> _eventTypes;
  private final HelixDataAccessor _accessor;
  private final ChangeType _changeType;
  private final HelixZkClient _zkClient;
  private final AtomicLong _lastNotificationTimeStamp;
  private final HelixManager _manager;
  private final PropertyKey _propertyKey;
  private boolean _batchModeEnabled = false;
  private boolean _preFetchEnabled = true;
  private HelixCallbackMonitor _monitor;

  // TODO: make this be per _manager or per _listener instaed of per callbackHandler -- Lei
  private CallbackProcessor _batchCallbackProcessor;
  private boolean _watchChild = true;  // Whether we should subscribe to the child znode's data change.

  // indicated whether this CallbackHandler is ready to serve event callback from ZkClient.
  private boolean _ready = false;

  static {
    SubscribeChangeEventProcessor =
        new DedupEventProcessor<CallbackHandler, SubscribeChangeEvent>("Singleton",
            "CallbackHanlder-AsycSubscribe") {
          @Override
          protected void handleEvent(SubscribeChangeEvent event) {
            logger.info("Resubscribe change listener to path: " + event.path + ", for listener: "
                + event.listener + ", watchChild: " + event.watchChild);
            try {
              if (event.handler.isReady()) {
                event.handler.subscribeForChanges(event.callbackType, event.path, event.watchChild);
              } else {
                logger.info(
                    "CallbackHandler is not ready, stop subscribing changes listener to path: "
                        + event.path + ", for listener: " + event.listener + ", watchChild: "
                        + event.watchChild);
              }
            } catch (Exception e) {
              logger.error("Failed to resubscribe change to path: " + event.path + " for listener "
                  + event.listener, e);
            }
          }
        };

    SubscribeChangeEventProcessor.start();
  }

  class SubscribeChangeEvent {
    final CallbackHandler handler;
    final String path;
    final NotificationContext.Type callbackType;
    final Object listener;
    final boolean watchChild;

    SubscribeChangeEvent(CallbackHandler handler, NotificationContext.Type callbackType,
        String path, boolean watchChild, Object listener) {
      this.handler = handler;
      this.path = path;
      this.callbackType = callbackType;
      this.listener = listener;
      this.watchChild = watchChild;
    }
  }

  class CallbackProcessor
      extends DedupEventProcessor<NotificationContext.Type, NotificationContext> {
    private CallbackHandler _handler;

    public CallbackProcessor(CallbackHandler handler) {
      super(_manager.getClusterName(), "CallbackProcessor");
      _handler = handler;
    }

    @Override
    protected void handleEvent(NotificationContext event) {
      try {
        _handler.invoke(event);
      } catch (Exception e) {
        logger.warn("Exception in callback processing thread. Skipping callback", e);
      }
    }
  }

  /**
   * maintain the expected notification types
   * this is fix for HELIX-195: race condition between FINALIZE callbacks and Zk callbacks
   */
  private List<NotificationContext.Type> _expectTypes = nextNotificationType.get(Type.FINALIZE);

  public CallbackHandler(HelixManager manager, HelixZkClient client, PropertyKey propertyKey,
      Object listener, EventType[] eventTypes, ChangeType changeType) {
    this(manager, client, propertyKey, listener, eventTypes, changeType, null);
  }

  public CallbackHandler(HelixManager manager, HelixZkClient client, PropertyKey propertyKey,
        Object listener, EventType[] eventTypes, ChangeType changeType,
        HelixCallbackMonitor monitor) {
    if (listener == null) {
      throw new HelixException("listener could not be null");
    }

    if (monitor != null && !monitor.getChangeType().equals(changeType)) {
      throw new HelixException(
          "The specified callback monitor is for different change type: " + monitor.getChangeType()
              .name());
    }

    _manager = manager;
    _accessor = manager.getHelixDataAccessor();
    _zkClient = client;
    _propertyKey = propertyKey;
    _path = propertyKey.getPath();
    _listener = listener;
    _eventTypes = new HashSet<>(Arrays.asList(eventTypes));
    _changeType = changeType;
    _lastNotificationTimeStamp = new AtomicLong(System.nanoTime());
    _monitor = monitor;

    if (_changeType == MESSAGE || _changeType == MESSAGES_CONTROLLER || _changeType == CONTROLLER) {
      _watchChild = false;
    } else {
      _watchChild = true;
    }

    parseListenerProperties();

    init();
  }


  private void parseListenerProperties() {
    BatchMode batchMode = _listener.getClass().getAnnotation(BatchMode.class);
    PreFetch preFetch = _listener.getClass().getAnnotation(PreFetch.class);

    String asyncBatchModeEnabled = System.getProperty(SystemPropertyKeys.ASYNC_BATCH_MODE_ENABLED);
    if (asyncBatchModeEnabled == null) {
      // for backcompatible, the old property name is deprecated.
      asyncBatchModeEnabled =
          System.getProperty(SystemPropertyKeys.LEGACY_ASYNC_BATCH_MODE_ENABLED);
    }

    if (asyncBatchModeEnabled != null) {
      _batchModeEnabled = Boolean.parseBoolean(asyncBatchModeEnabled);
      logger.info("isAsyncBatchModeEnabled by default: " + _batchModeEnabled);
    }

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
      case TARGET_EXTERNAL_VIEW:
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

  public void enqueueTask(NotificationContext changeContext)
      throws Exception {
    //async mode only applicable to CALLBACK from ZK, During INIT and FINALIZE invoke the callback's immediately.
    if (_batchModeEnabled && changeContext.getType() == NotificationContext.Type.CALLBACK) {
      logger.debug("Enqueuing callback");
      if (!isReady()) {
        logger.info(
            "CallbackHandler is not ready, ignore change callback from path: "
                + _path + ", for listener: " + _listener);
      } else {
        synchronized (this) {
          if (_batchCallbackProcessor != null) {
            _batchCallbackProcessor.queueEvent(changeContext.getType(), changeContext);
          } else {
            throw new HelixException("Failed to process callback in batch mode. Batch Callback Processor does not exist.");
          }
        }
      }
    } else {
      invoke(changeContext);
    }

    if (_monitor != null) {
      _monitor.increaseCallbackUnbatchedCounters();
    }
  }

  public void invoke(NotificationContext changeContext) throws Exception {
    Type type = changeContext.getType();
    long start = System.currentTimeMillis();

    // This allows the listener to work with one change at a time
    synchronized (_manager) {
      if (logger.isInfoEnabled()) {
        logger.info(
            Thread.currentThread().getId() + " START:INVOKE " + _path + " listener:" + _listener
                + " type: " + type);
      }

      if (!_expectTypes.contains(type)) {
        logger.warn(
            "Callback handler received event in wrong order. Listener: " + _listener + ", path: "
                + _path + ", expected types: " + _expectTypes + " but was " + type);
        return;

      }
      _expectTypes = nextNotificationType.get(type);

      if (type == Type.INIT || type == Type.FINALIZE) {
        subscribeForChanges(changeContext.getType(), _path, _watchChild);
      } else {
        // put SubscribeForChange run in async thread to reduce the latency of zk callback handling.
        subscribeForChangesAsyn(changeContext.getType(), _path, _watchChild);
      }

      if (_changeType == IDEAL_STATE) {
        IdealStateChangeListener idealStateChangeListener = (IdealStateChangeListener) _listener;
        List<IdealState> idealStates = preFetch(_propertyKey);
        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);
      } else if (_changeType == INSTANCE_CONFIG) {
        if (_listener instanceof ConfigChangeListener) {
          ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
          List<InstanceConfig> configs = preFetch(_propertyKey);
          configChangeListener.onConfigChange(configs, changeContext);
        } else if (_listener instanceof InstanceConfigChangeListener) {
          InstanceConfigChangeListener listener = (InstanceConfigChangeListener) _listener;
          List<InstanceConfig> configs = preFetch(_propertyKey);
          listener.onInstanceConfigChange(configs, changeContext);
        }
      } else if (_changeType == RESOURCE_CONFIG) {
        ResourceConfigChangeListener listener = (ResourceConfigChangeListener) _listener;
        List<ResourceConfig> configs = preFetch(_propertyKey);
        listener.onResourceConfigChange(configs, changeContext);

      } else if (_changeType == CLUSTER_CONFIG) {
        ClusterConfigChangeListener listener = (ClusterConfigChangeListener) _listener;
        ClusterConfig config = null;
        if (_preFetchEnabled) {
          config = _accessor.getProperty(_propertyKey);
        }
        listener.onClusterConfigChange(config, changeContext);

      } else if (_changeType == CONFIG) {
        ScopedConfigChangeListener listener = (ScopedConfigChangeListener) _listener;
        List<HelixProperty> configs = preFetch(_propertyKey);
        listener.onConfigChange(configs, changeContext);

      } else if (_changeType == LIVE_INSTANCE) {
        LiveInstanceChangeListener liveInstanceChangeListener = (LiveInstanceChangeListener) _listener;
        List<LiveInstance> liveInstances = preFetch(_propertyKey);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      } else if (_changeType == CURRENT_STATE) {
        CurrentStateChangeListener currentStateChangeListener = (CurrentStateChangeListener) _listener;
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<CurrentState> currentStates = preFetch(_propertyKey);
        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext);

      } else if (_changeType == MESSAGE) {
        MessageListener messageListener = (MessageListener) _listener;
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<Message> messages = preFetch(_propertyKey);
        messageListener.onMessage(instanceName, messages, changeContext);

      } else if (_changeType == MESSAGES_CONTROLLER) {
        MessageListener messageListener = (MessageListener) _listener;
        List<Message> messages = preFetch(_propertyKey);
        messageListener.onMessage(_manager.getInstanceName(), messages, changeContext);

      } else if (_changeType == EXTERNAL_VIEW || _changeType == TARGET_EXTERNAL_VIEW) {
        ExternalViewChangeListener externalViewListener = (ExternalViewChangeListener) _listener;
        List<ExternalView> externalViewList = preFetch(_propertyKey);
        externalViewListener.onExternalViewChange(externalViewList, changeContext);

      } else if (_changeType == CONTROLLER) {
        ControllerChangeListener controllerChangelistener = (ControllerChangeListener) _listener;
        controllerChangelistener.onControllerChange(changeContext);
      } else {
        logger.warn("Unknown change type: " + _changeType);
      }

      long end = System.currentTimeMillis();
      if (logger.isInfoEnabled()) {
        logger.info(
            Thread.currentThread().getId() + " END:INVOKE " + _path + " listener:" + _listener
                + " type: " + type + " Took: " + (end - start) + "ms");
      }
      if (_monitor != null) {
        _monitor.increaseCallbackCounters(end - start);
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

  private void subscribeChildChange(String path, NotificationContext.Type callbackType) {
    if (callbackType == NotificationContext.Type.INIT
        || callbackType == NotificationContext.Type.CALLBACK) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            _manager.getInstanceName() + " subscribes child-change. path: " + path + ", listener: " + _listener);
      }
      _zkClient.subscribeChildChanges(path, this);
    } else if (callbackType == NotificationContext.Type.FINALIZE) {
      logger.info(
          _manager.getInstanceName() + " unsubscribe child-change. path: " + path + ", listener: " + _listener);

      _zkClient.unsubscribeChildChanges(path, this);
    }
  }

  private void subscribeDataChange(String path, NotificationContext.Type callbackType) {
    if (callbackType == NotificationContext.Type.INIT
        || callbackType == NotificationContext.Type.CALLBACK) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            _manager.getInstanceName() + " subscribe data-change. path: " + path + ", listener: "
                + _listener);
      }
      _zkClient.subscribeDataChanges(path, this);
    } else if (callbackType == NotificationContext.Type.FINALIZE) {
      logger.info(
          _manager.getInstanceName() + " unsubscribe data-change. path: " + path + ", listener: "
              + _listener);

      _zkClient.unsubscribeDataChanges(path, this);
    }
  }

  /** Subscribe Changes in asynchronously */
  private void subscribeForChangesAsyn(NotificationContext.Type callbackType, String path, boolean watchChild) {
    SubscribeChangeEvent subscribeEvent =
        new SubscribeChangeEvent(this, callbackType, path, watchChild, _listener);
    SubscribeChangeEventProcessor.queueEvent(subscribeEvent.handler, subscribeEvent);
  }

  private void subscribeForChanges(NotificationContext.Type callbackType, String path,
      boolean watchChild) {
    logger.info(
        "Subscribing changes listener to path: " + path + ", type: " + callbackType + ", listener: "
            + _listener);

    long start = System.currentTimeMillis();
    if (_eventTypes.contains(EventType.NodeDataChanged) || _eventTypes
        .contains(EventType.NodeCreated) || _eventTypes.contains(EventType.NodeDeleted)) {
      logger.info("Subscribing data change listener to path: " + path);
      subscribeDataChange(path, callbackType);
    }

    if (_eventTypes.contains(EventType.NodeChildrenChanged)) {
      logger.info("Subscribing child change listener to path:" + path);
      subscribeChildChange(path, callbackType);
      if (watchChild) {
        logger.info("Subscribing data change listener to all children for path:" + path);

        try {
          switch (_changeType) {
          case CURRENT_STATE:
          case IDEAL_STATE:
          case EXTERNAL_VIEW:
          case TARGET_EXTERNAL_VIEW: {
            // check if bucketized
            BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
            List<ZNRecord> records = baseAccessor.getChildren(path, null, 0);
            for (ZNRecord record : records) {
              HelixProperty property = new HelixProperty(record);
              String childPath = path + "/" + record.getId();

              int bucketSize = property.getBucketSize();
              if (bucketSize > 0) {
                // subscribe both data-change and child-change on bucketized parent node
                // data-change gives a delete-callback which is used to remove watch
                subscribeChildChange(childPath, callbackType);
                subscribeDataChange(childPath, callbackType);

                // subscribe data-change on bucketized child
                List<String> bucketizedChildNames = _zkClient.getChildren(childPath);
                if (bucketizedChildNames != null) {
                  for (String bucketizedChildName : bucketizedChildNames) {
                    String bucketizedChildPath = childPath + "/" + bucketizedChildName;
                    subscribeDataChange(bucketizedChildPath, callbackType);
                  }
                }
              } else {
                subscribeDataChange(childPath, callbackType);
              }
            }
            break;
          }
          default: {
            List<String> childNames = _zkClient.getChildren(path);
            if (childNames != null) {
              for (String childName : childNames) {
                String childPath = path + "/" + childName;
                subscribeDataChange(childPath, callbackType);
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
    logger.info("Subscribing to path:" + path + " took:" + (end - start));
  }

  public EventType[] getEventTypes() {
    return (EventType[]) _eventTypes.toArray();
  }

  /**
   * Invoke the listener so that it sets up the initial values from the zookeeper if any
   * exists
   */
  public void init() {
    logger.info("initializing CallbackHandler: " + this.toString() + " content: " + getContent());

    if (_batchModeEnabled) {
      synchronized (this) {
        if (_batchCallbackProcessor != null) {
          _batchCallbackProcessor.resetEventQueue();
        } else {
          _batchCallbackProcessor = new CallbackProcessor(this);
          _batchCallbackProcessor.start();
        }
      }
    }

    updateNotificationTime(System.nanoTime());
    try {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.INIT);
      changeContext.setChangeType(_changeType);
      _ready = true;
      invoke(changeContext);
    } catch (Exception e) {
      String msg = "Exception while invoking init callback for listener:" + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data) {
    if (logger.isDebugEnabled()) {
      logger.debug("Data change callback: paths changed: " + dataPath);
    }

    try {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path)) {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        changeContext.setPathChanged(dataPath);
        changeContext.setChangeType(_changeType);
        enqueueTask(changeContext);
      }
    } catch (Exception e) {
      String msg = "exception in handling data-change. path: " + dataPath + ", listener: " + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) {
    if (logger.isDebugEnabled()) {
      logger.debug("Data change callback: path deleted: " + dataPath);
    }

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
      }
    } catch (Exception e) {
      String msg = "exception in handling data-delete-change. path: " + dataPath + ", listener: "
          + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) {
    if (logger.isDebugEnabled()) {
      logger.debug("Data change callback: child changed, path: {} , current child count: {}",
          parentPath, currentChilds == null ? 0 : currentChilds.size());
    }

    try {
      updateNotificationTime(System.nanoTime());
      if (parentPath != null && parentPath.startsWith(_path)) {
        if (currentChilds == null && parentPath.equals(_path)) {
          // _path has been removed, remove this listener
          // removeListener will call handler.reset(), which in turn call invoke() on FINALIZE type
          _manager.removeListener(_propertyKey, _listener);
        } else {
          NotificationContext changeContext = new NotificationContext(_manager);
          changeContext.setType(NotificationContext.Type.CALLBACK);
          changeContext.setPathChanged(parentPath);
          changeContext.setChangeType(_changeType);
          subscribeForChanges(changeContext.getType(), _path, _watchChild);
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
  @Deprecated
  public void reset() {
    reset(true);
  }

  void reset(boolean isShutdown) {
    logger.info("Resetting CallbackHandler: {}. Is resetting for shutdown: {}.", this.toString(),
        isShutdown);
    try {
      _ready = false;
      synchronized (this) {
        if (_batchCallbackProcessor != null) {
          if (isShutdown) {
            _batchCallbackProcessor.shutdown();
            _batchCallbackProcessor = null;
          } else {
            _batchCallbackProcessor.resetEventQueue();
          }
        }
      }
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.FINALIZE);
      changeContext.setChangeType(_changeType);
      invoke(changeContext);
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

  public boolean isReady() {
    return _ready;
  }

  public String getContent() {
    return "CallbackHandler{" +
        "_watchChild=" + _watchChild +
        ", _preFetchEnabled=" + _preFetchEnabled +
        ", _batchModeEnabled=" + _batchModeEnabled +
        ", _path='" + _path + '\'' +
        ", _listener=" + _listener +
        ", _changeType=" + _changeType +
        ", _manager=" + _manager +
        ", _zkClient=" + _zkClient +
        '}';
  }
}
