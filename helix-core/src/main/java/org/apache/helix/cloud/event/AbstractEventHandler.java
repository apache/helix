package org.apache.helix.cloud.event;

/**
 * This class is the the interface for singleton eventHandler.
 * User may implement their own eventHandler or use the default CloudEventHandler
 */
public interface AbstractEventHandler {
  void registerCloudEventListener(CloudEventListener listener);
  void unregisterCloudEventListener(CloudEventListener listener);
}
