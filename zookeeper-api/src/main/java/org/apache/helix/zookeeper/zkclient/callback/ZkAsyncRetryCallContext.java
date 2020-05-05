package org.apache.helix.zookeeper.zkclient.callback;

import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkAsyncRetryCallContext extends ZkAsyncCallMonitorContext {
  private static Logger LOG = LoggerFactory.getLogger(ZkAsyncRetryCallContext.class);
  private final ZkAsyncRetryThread _retryThread;
  private final CancellableZkAsyncCallback _cancellableCallback;

  /**
   * @param retryThread       The thread that executes the retry operation.
   *                          Note that retry in the ZkEventThread is not allowed to avoid dead lock.
   * @param callback          Cancellable asynchronous callback to notify when the retry is cancelled.
   * @param monitor           ZkClient monitor for update the operation result.
   * @param startTimeMilliSec Operation initialization time.
   * @param bytes             The data size in bytes that is involved in the operation.
   * @param isRead            True if the operation is readonly.
   */
  public ZkAsyncRetryCallContext(final ZkAsyncRetryThread retryThread,
      final CancellableZkAsyncCallback callback, final ZkClientMonitor monitor,
      long startTimeMilliSec, int bytes, boolean isRead) {
    super(monitor, startTimeMilliSec, bytes, isRead);
    _retryThread = retryThread;
    _cancellableCallback = callback;
  }

  /**
   * Request a retry.
   *
   * @return True if the request was sent successfully.
   */
  boolean requestRetry() {
    return _retryThread.sendRetryRequest(this);
  }

  /**
   * Notify the pending callback that retry has been cancelled.
   */
  void cancel() {
    _cancellableCallback.notifyCallers();
  }

  /**
   * The actual retry operation logic.
   */
  protected abstract void doRetry() throws Exception;
}
