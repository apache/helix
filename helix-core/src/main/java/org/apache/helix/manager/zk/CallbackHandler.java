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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateChangeListener;
import org.apache.helix.api.listeners.CustomizedStateConfigChangeListener;
import org.apache.helix.api.listeners.CustomizedStateRootChangeListener;
import org.apache.helix.api.listeners.CustomizedViewChangeListener;
import org.apache.helix.api.listeners.CustomizedViewRootChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.api.listeners.TaskCurrentStateChangeListener;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.mbeans.HelixCallbackMonitor;
import org.apache.helix.zookeeper.api.client.ChildrenSubscribeResult;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.annotation.PreFetchChangedData;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.HelixConstants.ChangeType.CLUSTER_CONFIG;
import static org.apache.helix.HelixConstants.ChangeType.CONFIG;
import static org.apache.helix.HelixConstants.ChangeType.CONTROLLER;
import static org.apache.helix.HelixConstants.ChangeType.CURRENT_STATE;
import static org.apache.helix.HelixConstants.ChangeType.CUSTOMIZED_STATE;
import static org.apache.helix.HelixConstants.ChangeType.CUSTOMIZED_STATE_CONFIG;
import static org.apache.helix.HelixConstants.ChangeType.CUSTOMIZED_STATE_ROOT;
import static org.apache.helix.HelixConstants.ChangeType.CUSTOMIZED_VIEW;
import static org.apache.helix.HelixConstants.ChangeType.CUSTOMIZED_VIEW_ROOT;
import static org.apache.helix.HelixConstants.ChangeType.EXTERNAL_VIEW;
import static org.apache.helix.HelixConstants.ChangeType.IDEAL_STATE;
import static org.apache.helix.HelixConstants.ChangeType.INSTANCE_CONFIG;
import static org.apache.helix.HelixConstants.ChangeType.LIVE_INSTANCE;
import static org.apache.helix.HelixConstants.ChangeType.MESSAGE;
import static org.apache.helix.HelixConstants.ChangeType.MESSAGES_CONTROLLER;
import static org.apache.helix.HelixConstants.ChangeType.RESOURCE_CONFIG;
import static org.apache.helix.HelixConstants.ChangeType.TARGET_EXTERNAL_VIEW;
import static org.apache.helix.HelixConstants.ChangeType.TASK_CURRENT_STATE;

@PreFetchChangedData(enabled = false)
public class CallbackHandler implements IZkChildListener, IZkDataListener {
  private static Logger logger = LoggerFactory.getLogger(CallbackHandler.class);
  private static final AtomicLong CALLBACK_HANDLER_UID = new AtomicLong();

  private final long _uid;
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
  private final RealmAwareZkClient _zkClient;
  private final AtomicLong _lastNotificationTimeStamp;
  private final HelixManager _manager;
  private final PropertyKey _propertyKey;
  private boolean _batchModeEnabled = false;
  private boolean _preFetchEnabled = true;
  private HelixCallbackMonitor _monitor;

  private AtomicReference<CallbackEventExecutor> _batchCallbackExecutorRef = new AtomicReference<>();
  private boolean _watchChild = true; // Whether we should subscribe to the child znode's data
  // change.

  // indicated whether this CallbackHandler is ready to serve event callback from ZkClient.
  private boolean _ready = false;

  /**
   * maintain the expected notification types
   * this is fix for HELIX-195: race condition between FINALIZE callbacks and Zk callbacks
   */
  private List<NotificationContext.Type> _expectTypes = nextNotificationType.get(Type.FINALIZE);

  public CallbackHandler(HelixManager manager, RealmAwareZkClient client, PropertyKey propertyKey,
      Object listener, EventType[] eventTypes, ChangeType changeType) {
    this(manager, client, propertyKey, listener, eventTypes, changeType, null);
  }

  public CallbackHandler(HelixManager manager, RealmAwareZkClient client, PropertyKey propertyKey,
      Object listener, EventType[] eventTypes, ChangeType changeType,
      HelixCallbackMonitor monitor) {
    if (listener == null) {
      throw new HelixException("listener could not be null");
    }

    if (monitor != null && !monitor.getChangeType().equals(changeType)) {
      throw new HelixException("The specified callback monitor is for different change type: "
          + monitor.getChangeType().name());
    }

    _uid = CALLBACK_HANDLER_UID.getAndIncrement();


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
      logger.info("isAsyncBatchModeEnabled by default: {}", _batchModeEnabled);
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
      case CUSTOMIZED_STATE_CONFIG:
        listenerClass = CustomizedStateConfigChangeListener.class;
        break;
      case CONFIG:
        listenerClass = ConfigChangeListener.class;
        break;
      case LIVE_INSTANCE:
        listenerClass = LiveInstanceChangeListener.class;
        break;
      case CURRENT_STATE:
        listenerClass = CurrentStateChangeListener.class;
        break;
      case TASK_CURRENT_STATE:
        listenerClass = TaskCurrentStateChangeListener.class;
        break;
      case CUSTOMIZED_STATE_ROOT:
        listenerClass = CustomizedStateRootChangeListener.class;
        break;
      case CUSTOMIZED_STATE:
        listenerClass = CustomizedStateChangeListener.class;
        break;
      case MESSAGE:
      case MESSAGES_CONTROLLER:
        listenerClass = MessageListener.class;
        break;
      case EXTERNAL_VIEW:
      case TARGET_EXTERNAL_VIEW:
        listenerClass = ExternalViewChangeListener.class;
        break;
      case CUSTOMIZED_VIEW:
        listenerClass = CustomizedViewChangeListener.class;
        break;
      case CUSTOMIZED_VIEW_ROOT:
        listenerClass = CustomizedViewRootChangeListener.class;
        break;
      case CONTROLLER:
        listenerClass = ControllerChangeListener.class;
    }

    Method callbackMethod = listenerClass.getMethods()[0];
    try {
      Method method = _listener.getClass().getMethod(callbackMethod.getName(),
          callbackMethod.getParameterTypes());
      BatchMode batchModeInMethod = method.getAnnotation(BatchMode.class);
      PreFetch preFetchInMethod = method.getAnnotation(PreFetch.class);
      if (batchModeInMethod != null) {
        _batchModeEnabled = batchModeInMethod.enabled();
      }
      if (preFetchInMethod != null) {
        _preFetchEnabled = preFetchInMethod.enabled();
      }
    } catch (NoSuchMethodException e) {
      logger.warn("No method {} defined in listener {}", callbackMethod.getName(),
          _listener.getClass().getCanonicalName());
    }
  }

  public Object getListener() {
    return _listener;
  }

  public String getPath() {
    return _path;
  }

  public void enqueueTask(NotificationContext changeContext) throws Exception {
    // async mode only applicable to CALLBACK from ZK, During INIT and FINALIZE invoke the
    // callback's immediately.
    if (_batchModeEnabled && changeContext.getType() == NotificationContext.Type.CALLBACK) {
      logger.debug("Callbackhandler {}, Enqueuing callback", _uid );
      if (!isReady()) {
        logger.info("CallbackHandler {} is not ready, ignore change callback from path: {}, for "
            + "listener: {}", _uid, _path, _listener);
      } else {
        // submit
        CallbackEventExecutor callbackProcessor = _batchCallbackExecutorRef.get();
        if (callbackProcessor != null) {
          callbackProcessor.submitEventToExecutor(changeContext.getType(), changeContext, this);
        } else {
          throw new HelixException(
              "Failed to process callback in batch mode. Batch Callback Processor does not exist.");
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
    if (logger.isInfoEnabled()) {
      logger.info("{} START: CallbackHandler {}, INVOKE {} listener: {} type: {}",
          Thread.currentThread().getId(), _uid, _path, _listener, type);
    }

    synchronized (this) {
      if (!_expectTypes.contains(type)) {
        logger.warn("Callback handler {} received event in wrong order. Listener: {}, path: {}, "
            + "expected types: {}, but was {}", _uid, _listener, _path, _expectTypes, type);
        return;
      }
      _expectTypes = nextNotificationType.get(type);

      if (type == Type.INIT || type == Type.FINALIZE || changeContext.getIsChildChange()) {
        subscribeForChanges(changeContext.getType(), _path, _watchChild);
      }
    }

    // This allows the Helix Manager to work with one change at a time
    // TODO: Maybe we don't need to sync on _manager for all types of listener. PCould be a
    // potential improvement candidate.
    synchronized (_manager) {
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

      } else if (_changeType == CUSTOMIZED_STATE_CONFIG) {
        CustomizedStateConfigChangeListener listener = (CustomizedStateConfigChangeListener) _listener;
        CustomizedStateConfig config = null;
        if (_preFetchEnabled) {
          config = _accessor.getProperty(_propertyKey);
        }
        listener.onCustomizedStateConfigChange(config, changeContext);

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
        LiveInstanceChangeListener liveInstanceChangeListener =
            (LiveInstanceChangeListener) _listener;
        List<LiveInstance> liveInstances = preFetch(_propertyKey);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);

      } else if (_changeType == CURRENT_STATE) {
        CurrentStateChangeListener currentStateChangeListener =
            (CurrentStateChangeListener) _listener;
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<CurrentState> currentStates = preFetch(_propertyKey);
        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext);

      } else if (_changeType == TASK_CURRENT_STATE) {
        TaskCurrentStateChangeListener taskCurrentStateChangeListener =
            (TaskCurrentStateChangeListener) _listener;
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<CurrentState> currentStates = preFetch(_propertyKey);
        taskCurrentStateChangeListener
            .onTaskCurrentStateChange(instanceName, currentStates, changeContext);

      } else if (_changeType == CUSTOMIZED_STATE_ROOT) {
        CustomizedStateRootChangeListener customizedStateRootChangeListener =
            (CustomizedStateRootChangeListener) _listener;
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<String> customizedStateTypes = new ArrayList<>();
        if (_preFetchEnabled) {
          customizedStateTypes =
              _accessor.getChildNames(_accessor.keyBuilder().customizedStatesRoot(instanceName));
        }
        customizedStateRootChangeListener
            .onCustomizedStateRootChange(instanceName, customizedStateTypes, changeContext);

      } else if (_changeType == CUSTOMIZED_STATE) {
        CustomizedStateChangeListener customizedStateChangeListener =
            (CustomizedStateChangeListener) _listener;
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<CustomizedState> customizedStates = preFetch(_propertyKey);
        customizedStateChangeListener.onCustomizedStateChange(instanceName, customizedStates, changeContext);

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

      } else if (_changeType == CUSTOMIZED_VIEW_ROOT) {
        CustomizedViewRootChangeListener customizedViewRootChangeListener =
            (CustomizedViewRootChangeListener) _listener;
        List<String> customizedViewTypes = new ArrayList<>();
        if (_preFetchEnabled) {
          customizedViewTypes = _accessor.getChildNames(_accessor.keyBuilder().customizedViews());
        }
        customizedViewRootChangeListener.onCustomizedViewRootChange(customizedViewTypes,
            changeContext);

      } else if (_changeType == CUSTOMIZED_VIEW) {
        CustomizedViewChangeListener customizedViewListener = (CustomizedViewChangeListener) _listener;
        List<CustomizedView> customizedViewListList = preFetch(_propertyKey);
        customizedViewListener.onCustomizedViewChange(customizedViewListList, changeContext);

      } else if (_changeType == CONTROLLER) {
        ControllerChangeListener controllerChangelistener = (ControllerChangeListener) _listener;
        controllerChangelistener.onControllerChange(changeContext);
      } else {
        logger.warn("Callbackhandler {}, Unknown change type: {}", _uid, _changeType);
      }

      long end = System.currentTimeMillis();
      if (logger.isInfoEnabled()) {
        logger.info("{} END:INVOKE CallbackHandler {}, {} listener: {} type: {} Took: {}ms",
            Thread.currentThread().getId(), _uid, _path, _listener, type, (end - start));
      }
      if (_monitor != null) {
        _monitor.increaseCallbackCounters(end - start);
      }
    }
  }

  private <T extends HelixProperty> List<T> preFetch(PropertyKey key) {
    if (_preFetchEnabled) {
      return _accessor.getChildValues(key, true);
    } else {
      return Collections.emptyList();
    }
  }

  /*
   * If callback type is INIT or CALLBACK, subscribes child change listener to the path
   * and returns the path's children names. The children list might be null when the path
   * doesn't exist or callback type is INIT/CALLBACK.
   */
  private List<String> subscribeChildChange(String path, NotificationContext.Type callbackType) {
    if (callbackType == NotificationContext.Type.INIT
        || callbackType == NotificationContext.Type.CALLBACK) {
      if (logger.isDebugEnabled()) {
        logger.debug("CallbackHandler {}, {} subscribes child-change. path: {} , listener: {}",
            _uid, _manager.getInstanceName(), path, _listener );
      }
      // In the lifecycle of CallbackHandler, INIT is the first stage of registration of watch.
      // For some usage case such as current state, the path can be created later. Thus we would
      // install watch anyway event the path is not yet created.
      // Later, CALLBACK type, the CallbackHandler already registered the watch and knows the
      // path was created. Here, to avoid leaking path in ZooKeeper server, we would not let
      // CallbackHandler to install exists watch, namely watch for path not existing.
      // Note when path is removed, the CallbackHandler would remove itself from ZkHelixManager too
      // to avoid leaking a CallbackHandler.
      ChildrenSubscribeResult childrenSubscribeResult =
          _zkClient.subscribeChildChanges(path, this, callbackType != Type.INIT);
      logger.debug("CallbackHandler {} subscribe data path {} result {}", _uid, path,
          childrenSubscribeResult.isInstalled());
      if (!childrenSubscribeResult.isInstalled()) {
        logger.info("CallbackHandler {} subscribe data path {} failed!", _uid, path);
      }
      // getChildren() might be null: when path doesn't exist.
      return childrenSubscribeResult.getChildren();
    } else if (callbackType == NotificationContext.Type.FINALIZE) {
      logger.info("CallbackHandler{}, {} unsubscribe child-change. path: {}, listener: {}",
          _uid ,_manager.getInstanceName(), path, _listener);

      _zkClient.unsubscribeChildChanges(path, this);
    }

    try {
      return _zkClient.getChildren(path);
    } catch (ZkNoNodeException e) {
      return null;
    }
  }

  private void subscribeDataChange(String path, NotificationContext.Type callbackType) {
    if (callbackType == NotificationContext.Type.INIT
        || callbackType == NotificationContext.Type.CALLBACK) {
      if (logger.isDebugEnabled()) {
        logger.debug("CallbackHandler {}, {} subscribe data-change. path: {}, listener: {}",
            _uid, _manager.getInstanceName(), path, _listener);
      }
      boolean subStatus = _zkClient.subscribeDataChanges(path, this, callbackType != Type.INIT);
      logger.debug("CallbackHandler {} subscribe data path {} result {}", _uid, path, subStatus);
      if (!subStatus) {
        logger.info("CallbackHandler {} subscribe data path {} failed!", _uid, path);
      }
    } else if (callbackType == NotificationContext.Type.FINALIZE) {
      logger.info("CallbackHandler{}, {} unsubscribe data-change. path: {}, listener: {}",
          _uid, _manager.getInstanceName(), path, _listener);

      _zkClient.unsubscribeDataChanges(path, this);
    }
  }

  private void subscribeForChanges(NotificationContext.Type callbackType, String path,
      boolean watchChild) {
    logger.info("CallbackHandler {} subscribing changes listener to path: {}, callback type: {}, "
            + "event types: {}, listener: {}, watchChild: {}",
        _uid, path, callbackType, _eventTypes, _listener, watchChild);

    long start = System.currentTimeMillis();
    if (_eventTypes.contains(EventType.NodeDataChanged)
        || _eventTypes.contains(EventType.NodeCreated)
        || _eventTypes.contains(EventType.NodeDeleted)) {
      logger.info("CallbackHandler {} subscribing data change listener to path: {}", _uid, path);
      subscribeDataChange(path, callbackType);
    }

    if (_eventTypes.contains(EventType.NodeChildrenChanged)) {
      List<String> children = subscribeChildChange(path, callbackType);
      if (watchChild) {
        try {
          switch (_changeType) {
            case CURRENT_STATE:
            case TASK_CURRENT_STATE:
            case CUSTOMIZED_STATE:
            case IDEAL_STATE:
            case EXTERNAL_VIEW:
            case CUSTOMIZED_VIEW:
            case TARGET_EXTERNAL_VIEW: {
              // check if bucketized
              BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_zkClient);
              List<ZNRecord> records = baseAccessor.getChildren(path, null, 0, 0, 0);
              for (ZNRecord record : records) {
                HelixProperty property = new HelixProperty(record);
                String childPath = path + "/" + record.getId();

                int bucketSize = property.getBucketSize();
                if (bucketSize > 0) {
                  // subscribe both data-change and child-change on bucketized parent node
                  // data-change gives a delete-callback which is used to remove watch
                  List<String> bucketizedChildNames = subscribeChildChange(childPath, callbackType);
                  subscribeDataChange(childPath, callbackType);

                  // subscribe data-change on bucketized child
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
              if (children != null) {
                for (String child : children) {
                  String childPath = path + "/" + child;
                  subscribeDataChange(childPath, callbackType);
                }
              }
              break;
            }
          }
        } catch (ZkNoNodeException | HelixMetaDataAccessException e) {
          //TODO: avoid calling getChildren for path that does not exist
          if (_changeType == CUSTOMIZED_STATE_ROOT) {
            logger.warn(
                "CallbackHandler {}, Failed to subscribe child/data change on path: {}, listener: {}. Instance "
                    + "does not support Customized State!", _uid, path, _listener);
          } else {
            logger.warn("CallbackHandler {}, Failed to subscribe child/data change. path: {}, listener: {}",
                _uid, path, _listener, e);
          }
        }
      }
    }

    long end = System.currentTimeMillis();
    logger.info("CallbackHandler{}, Subscribing to path: {} took: {}", _uid, path, (end - start));
  }

  public EventType[] getEventTypes() {
    return (EventType[]) _eventTypes.toArray();
  }

  /**
   * Invoke the listener so that it sets up the initial values from the zookeeper if any
   * exists
   */
  public void init() {
    logger.info("initializing CallbackHandler: {}, content: {} ", _uid, getContent());
    try {
      if (_batchModeEnabled) {
        CallbackEventExecutor callbackExecutor = _batchCallbackExecutorRef.get();
        if (callbackExecutor != null) {
          callbackExecutor.reset();
        } else {
          callbackExecutor = new CallbackEventExecutor(_manager);
          if (!_batchCallbackExecutorRef.compareAndSet(null, callbackExecutor)) {
            callbackExecutor.unregisterFromFactory();
          }
        }
      }
      updateNotificationTime(System.nanoTime());
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
      logger.debug("Data change callbackhandler {}: paths changed: {}", _uid, dataPath);
    }

    try {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path)) {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        changeContext.setPathChanged(dataPath);
        changeContext.setChangeType(_changeType);
        changeContext.setIsChildChange(false);
        enqueueTask(changeContext);
      }
    } catch (Exception e) {
      String msg =
          "exception in handling data-change. path: " + dataPath + ", listener: " + _listener;
      ZKExceptionHandler.getInstance().handle(msg, e);
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) {
    if (logger.isDebugEnabled()) {
      logger.debug("Data change callbackhandler {}: path deleted: {}", _uid, dataPath);
    }

    try {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path)) {
        logger.info("CallbackHandler {}, {} unsubscribe data-change. path: {}, listener: {}",
            _uid, _manager.getInstanceName(), dataPath, _listener);
        _zkClient.unsubscribeDataChanges(dataPath, this);

        // only needed for bucketized parent, but OK if we don't have child-change
        // watch on the bucketized parent path
        logger.info("CallbackHandler {}, {} unsubscribe child-change. path: {}, listener: {}",
            _uid, _manager.getInstanceName(), dataPath, _listener);
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
          boolean rt = _manager.removeListener(_propertyKey, _listener);
          logger.info("CallbackHandler {} removed with status {}", _uid, rt);
        } else {
          if (!isReady()) {
            // avoid leaking CallbackHandler
            logger.info("Callbackhandler {} with path {} is in reset state. Stop subscription to ZK client to avoid leaking",
                this, parentPath);
            return;
          }
          NotificationContext changeContext = new NotificationContext(_manager);
          changeContext.setType(NotificationContext.Type.CALLBACK);
          changeContext.setPathChanged(parentPath);
          changeContext.setChangeType(_changeType);
          changeContext.setIsChildChange(true);
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
    logger.info("Resetting CallbackHandler: {}. Is resetting for shutdown: {}.", _uid, isShutdown);
    try {
      _ready = false;
      CallbackEventExecutor callbackExecutor = _batchCallbackExecutorRef.get();
      if (callbackExecutor != null) {
        if (isShutdown) {
          if (_batchCallbackExecutorRef.compareAndSet(callbackExecutor, null)) {
            callbackExecutor.unregisterFromFactory();
          }
        } else {
          callbackExecutor.reset();
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
    return "CallbackHandler{" + "_watchChild=" + _watchChild + ", _preFetchEnabled="
        + _preFetchEnabled + ", _batchModeEnabled=" + _batchModeEnabled + ", _path='" + _path + '\''
        + ", _listener=" + _listener + ", _changeType=" + _changeType + ", _manager=" + _manager
        + ", _zkClient=" + _zkClient + '}';
  }
}
