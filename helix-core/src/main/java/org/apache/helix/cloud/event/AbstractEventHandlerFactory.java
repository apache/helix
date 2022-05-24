package org.apache.helix.cloud.event;

public interface AbstractEventHandlerFactory {
  AbstractEventHandler getInstanceObjectFunction();
}
