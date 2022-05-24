package org.apache.helix.cloud.event;

public final class HelixTestCloudEventHandlerFactory implements AbstractEventHandlerFactory {
  private static HelixTestCloudEventHandler _INSTANCE = null;

  /**
   * Get a HelixTestCloudEventHandler instance
   * @return an instance of HelixTestCloudEventHandler
   */
  @Override
  public HelixTestCloudEventHandler getInstanceObjectFunction() {
    if (_INSTANCE == null) {
      synchronized (HelixTestCloudEventHandler.class) {
        if (_INSTANCE == null) {
          _INSTANCE = new HelixTestCloudEventHandler();
        }
      }
    }
    return _INSTANCE;
  }

  public static HelixTestCloudEventHandler getInstance(){
    if (_INSTANCE == null) {
      synchronized (HelixTestCloudEventHandler.class) {
        if (_INSTANCE == null) {
          _INSTANCE = new HelixTestCloudEventHandler();
        }
      }
    }
    return _INSTANCE;
  }
}
