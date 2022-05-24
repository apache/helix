package org.apache.helix.cloud.event;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.helix.HelixException;
import org.apache.helix.cloud.event.helix.HelixCloudEventListener;

public class HelixTestCloudEventHandler extends CloudEventHandler {
  private static final int TIMEOUT = 900;  // second to timeout
  private static ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Override
  public void registerCloudEventListener(CloudEventListener listener) {
    super.registerCloudEventListener(listener);
  }
  private void executeEvent(HelixCloudEventListener.EventType eventType, Object eventInfo) {
    Future task = executorService.submit(() -> {
      performAction(eventType, eventInfo);
    });
    try {
      task.get(TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
      throw new HelixException("HelixCloudEventHandler failed to execute action. Exception: ", e);
    }
  }
}