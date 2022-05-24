package org.apache.helix.cloud.event;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HelixTestCloudEventHandler extends CloudEventHandler {
  private static final int TIMEOUT = 900;  // second to timeout
  private static ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Override
  public void registerCloudEventListener(CloudEventListener listener) {
    super.registerCloudEventListener(listener);
    System.out.println("in child register");
  }


}