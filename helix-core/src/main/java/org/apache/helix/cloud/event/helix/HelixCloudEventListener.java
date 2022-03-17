package org.apache.helix.cloud.event.helix;

import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.cloud.event.CloudEventListener;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixCloudEventListener implements CloudEventListener {
  private static Logger LOG = LoggerFactory.getLogger(HelixCloudEventListener.class);

  private final CloudEventCallbackProperty _property;
  private final DefaultCloudEventCallbackImpl _callbackImplClass;
  private final HelixManager _helixManager;

  public HelixCloudEventListener(CloudEventCallbackProperty property, HelixManager helixManager)
      throws InstantiationException, IllegalAccessException {
    this._property = property;
    this._helixManager = helixManager;
    this._callbackImplClass = loadCloudEventCallbackImplClass(property.getUserArgs()
        .getOrDefault(CloudEventCallbackProperty.UserArgsInputKey.CALLBACK_IMPL_CLASS_NAME,
            DefaultCloudEventCallbackImpl.class.getCanonicalName()));
  }

  public enum EventType {
    ON_PAUSE, ON_RESUME
  }

  /**
   * Below are enums defining the sequence of callbacks for each type of events
   */
  private enum OnPauseOperations {
    PRE_ON_PAUSE {
      @Override
      public CloudEventCallbackProperty.UserDefinedCallbackType toUserDefinedCallbackType() {
        return CloudEventCallbackProperty.UserDefinedCallbackType.PRE_ON_PAUSE;
      }
    }, MAINTENANCE_MODE, ENABLE_DISABLE_INSTANCE, POST_ON_PAUSE {
      @Override
      public CloudEventCallbackProperty.UserDefinedCallbackType toUserDefinedCallbackType() {
        return CloudEventCallbackProperty.UserDefinedCallbackType.POST_ON_PAUSE;
      }
    };

    public CloudEventCallbackProperty.UserDefinedCallbackType toUserDefinedCallbackType() {
      return null;
    }
  }

  private enum OnResumeOperations {
    PRE_ON_RESUME {
      @Override
      public CloudEventCallbackProperty.UserDefinedCallbackType toUserDefinedCallbackType() {
        return CloudEventCallbackProperty.UserDefinedCallbackType.PRE_ON_RESUME;
      }
    }, ENABLE_DISABLE_INSTANCE, MAINTENANCE_MODE, POST_ON_RESUME {
      @Override
      public CloudEventCallbackProperty.UserDefinedCallbackType toUserDefinedCallbackType() {
        return CloudEventCallbackProperty.UserDefinedCallbackType.POST_ON_RESUME;
      }
    };

    public CloudEventCallbackProperty.UserDefinedCallbackType toUserDefinedCallbackType() {
      return null;
    }
  }

  @Override
  public void performAction(Object eventType, Object eventInfo) {
    LOG.info("Received {} event, event info {}, timestamp {}. Acting on the event... "
            + "Actor {}, based on callback implementation class {}.", ((EventType) eventType).name(),
        eventInfo == null ? "N/A" : eventInfo.toString(), System.currentTimeMillis(), _helixManager,
        _callbackImplClass.getClass().getCanonicalName());
    Set<CloudEventCallbackProperty.HelixOperation> enabledHelixOperationSet =
        _property.getEnabledHelixOperation();

    if (eventType == EventType.ON_PAUSE) {
      for (OnPauseOperations operation : OnPauseOperations.values()) {
        switch (operation) {
          case ENABLE_DISABLE_INSTANCE:
            if (enabledHelixOperationSet
                .contains(CloudEventCallbackProperty.HelixOperation.ENABLE_DISABLE_INSTANCE)) {
              _callbackImplClass.disableInstance(_helixManager, eventInfo);
            }
            break;
          case MAINTENANCE_MODE:
            if (enabledHelixOperationSet
                .contains(CloudEventCallbackProperty.HelixOperation.MAINTENANCE_MODE)) {
              _callbackImplClass.enterMaintenanceMode(_helixManager, eventInfo);
            }
            break;
          default:
            if (operation.toUserDefinedCallbackType() != null) {
              _property.getUserDefinedCallbackMap()
                  .getOrDefault(operation.toUserDefinedCallbackType(), (manager, info) -> {
                  }).accept(_helixManager, eventInfo);
            }
        }
      }
    } else if (eventType == EventType.ON_RESUME) {
      for (OnResumeOperations operation : OnResumeOperations.values()) {
        switch (operation) {
          case ENABLE_DISABLE_INSTANCE:
            if (enabledHelixOperationSet
                .contains(CloudEventCallbackProperty.HelixOperation.ENABLE_DISABLE_INSTANCE)) {
              _callbackImplClass.enableInstance(_helixManager, eventInfo);
            }
            break;
          case MAINTENANCE_MODE:
            if (enabledHelixOperationSet
                .contains(CloudEventCallbackProperty.HelixOperation.MAINTENANCE_MODE)) {
              _callbackImplClass.exitMaintenanceMode(_helixManager, eventInfo);
            }
            break;
          default:
            if (operation.toUserDefinedCallbackType() != null) {
              _property.getUserDefinedCallbackMap()
                  .getOrDefault(operation.toUserDefinedCallbackType(), (manager, info) -> {
                  }).accept(_helixManager, eventInfo);
            }
        }
      }
    }
  }

  @Override
  public CloudEventListener.ListenerType getListenerType() {
    return CloudEventListener.ListenerType.UNORDERED;
  }

  private DefaultCloudEventCallbackImpl loadCloudEventCallbackImplClass(String implClassName)
      throws IllegalAccessException, InstantiationException {
    DefaultCloudEventCallbackImpl implClass;
    try {
      LOG.info("Loading class: " + implClassName);
      implClass = (DefaultCloudEventCallbackImpl) HelixUtil.loadClass(getClass(), implClassName)
          .newInstance();
    } catch (Exception e) {
      implClass = DefaultCloudEventCallbackImpl.class.newInstance();
      LOG.error(
          "No cloud event callback implementation class found for: {}. message: {}. Using default callback impl class instead.",
          implClassName, e.getMessage());
    }
    LOG.info("Using {} as cloud event callback impl class.",
        implClass.getClass().getCanonicalName());
    return implClass;
  }
}
