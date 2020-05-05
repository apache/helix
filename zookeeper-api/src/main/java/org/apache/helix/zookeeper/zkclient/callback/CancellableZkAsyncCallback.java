package org.apache.helix.zookeeper.zkclient.callback;

public interface CancellableZkAsyncCallback {
  /**
   * Notify all the callers that are waiting for the callback to cancel the wait.
   */
  void notifyCallers();
}
