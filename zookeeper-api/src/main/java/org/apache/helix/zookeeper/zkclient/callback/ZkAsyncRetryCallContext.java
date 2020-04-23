package org.apache.helix.zookeeper.zkclient.callback;

import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;

public abstract class ZkAsyncRetryCallContext extends ZkAsyncCallMonitorContext {
  private final ZkAsyncRetryThread _retryThread;

  /**
   * @param retryThread The thread that executes the retry operation.
   *                    Note that retry in the ZkEventThread is not allowed to avoid dead lock.
   * @param monitor     ZkClient monitor for update the operation result.
   * @param startTimeMilliSec Operation initialization time.
   * @param bytes       The data size in bytes that is involved in the operation.
   * @param isRead      True if the operation is readonly.
   */
  public ZkAsyncRetryCallContext(final ZkAsyncRetryThread retryThread,
      final ZkClientMonitor monitor, long startTimeMilliSec, int bytes, boolean isRead) {
    super(monitor, startTimeMilliSec, bytes, isRead);
    _retryThread = retryThread;
  }

  /**
   * Request a retry.
   * @return True if the request was sent successfully.
   */
  boolean requestRetry() {
    return _retryThread.sendRetryRequest(this);
  }

  /**
   * The actual retry operation logic.
   */
  protected abstract void doRetry();
}
