package org.apache.helix.cloud.event.helix;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.cloud.event.CloudEventListener;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.cloud.event.helix.CloudEventCallbackProperty.HelixOperation;
import static org.apache.helix.cloud.event.helix.CloudEventCallbackProperty.UserDefinedCallbackType;

/**
 * A helix manager-based cloud event listener implementation
 */
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

  /**
   * The type of incoming event
   */
  public enum EventType {
    ON_PAUSE, ON_RESUME
  }

  /**
   * Below are lists defining the type and sequence of callbacks for each type of events
   */
  private final List<Object> onPauseOperations = Arrays
      .asList(UserDefinedCallbackType.PRE_ON_PAUSE, HelixOperation.MAINTENANCE_MODE,
          HelixOperation.ENABLE_DISABLE_INSTANCE, UserDefinedCallbackType.POST_ON_PAUSE);
  private final List<Object> onResumeOperations = Arrays
      .asList(UserDefinedCallbackType.PRE_ON_RESUME, HelixOperation.ENABLE_DISABLE_INSTANCE,
          HelixOperation.MAINTENANCE_MODE, UserDefinedCallbackType.POST_ON_RESUME);

  @Override
  public void performAction(Object eventType, Object eventInfo) {
    LOG.info("Received {} event, event info {}, timestamp {}. Acting on the event... "
            + "Actor {}, based on callback implementation class {}.", ((EventType) eventType).name(),
        eventInfo == null ? "N/A" : eventInfo.toString(), System.currentTimeMillis(), _helixManager,
        _callbackImplClass.getClass().getCanonicalName());

    if (eventType == EventType.ON_PAUSE) {
      onPauseOperations.forEach(operation -> executeOperation(eventType, eventInfo, operation));
    } else if (eventType == EventType.ON_RESUME) {
      onResumeOperations.forEach(operation -> executeOperation(eventType, eventInfo, operation));
    }
  }

  private void executeOperation(Object eventType, Object eventInfo, Object operation) {
    Set<CloudEventCallbackProperty.HelixOperation> enabledHelixOperationSet =
        _property.getEnabledHelixOperation();
    if (HelixOperation.ENABLE_DISABLE_INSTANCE.equals(operation)) {
      if (enabledHelixOperationSet.contains(HelixOperation.ENABLE_DISABLE_INSTANCE)) {
        if (eventType == EventType.ON_PAUSE) {
          _callbackImplClass.disableInstance(_helixManager, eventInfo);
        } else {
          _callbackImplClass.enableInstance(_helixManager, eventInfo);
        }
      }
    } else if (HelixOperation.MAINTENANCE_MODE.equals(operation)) {
      if (enabledHelixOperationSet.contains(HelixOperation.MAINTENANCE_MODE)) {
        if (eventType == EventType.ON_PAUSE) {
          _callbackImplClass.enterMaintenanceMode(_helixManager, eventInfo);
        } else {
          _callbackImplClass.exitMaintenanceMode(_helixManager, eventInfo);
        }
      }
    } else if (operation instanceof UserDefinedCallbackType) {
      BiConsumer<HelixManager, Object> callback =
          _property.getUserDefinedCallbackMap().get(operation);
      if (callback != null) {
        callback.accept(_helixManager, eventInfo);
      }
    } else {
      // Should not reach here
      throw new HelixException("Unknown category of cloud event operation " + operation.toString());
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
          .getConstructor().newInstance();
    } catch (Exception e) {
      implClass = new DefaultCloudEventCallbackImpl();
      LOG.error(
          "No cloud event callback implementation class found for: {}. message: {}. Using default callback impl class instead.",
          implClassName, e.getMessage());
    }
    LOG.info("Using {} as cloud event callback impl class.",
        implClass.getClass().getCanonicalName());
    return implClass;
  }
}
