package org.apache.helix.zookeeper.zkclient.callback;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
   * @param isCompressed      True if the data is compressed.
   */
  public ZkAsyncRetryCallContext(final ZkAsyncRetryThread retryThread,
      final CancellableZkAsyncCallback callback, final ZkClientMonitor monitor,
      long startTimeMilliSec, int bytes, boolean isRead, boolean isCompressed) {
    super(monitor, startTimeMilliSec, bytes, isRead, isCompressed);
    _retryThread = retryThread;
    _cancellableCallback = callback;
  }

  public ZkAsyncRetryCallContext(final ZkAsyncRetryThread retryThread,
      final CancellableZkAsyncCallback callback, final ZkClientMonitor monitor,
      long startTimeMilliSec, int bytes, boolean isRead) {
    this(retryThread, callback, monitor, startTimeMilliSec, bytes, isRead, false);
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
