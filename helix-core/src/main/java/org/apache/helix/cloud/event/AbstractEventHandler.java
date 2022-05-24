package org.apache.helix.cloud.event;

public interface AbstractEventHandler {
  void registerCloudEventListener(CloudEventListener listener);
  void unregisterCloudEventListener(CloudEventListener listener);
}
