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

public class ZkAsyncCallMonitorContext {
  private final long _startTimeMilliSec;
  private final ZkClientMonitor _monitor;
  private final boolean _isRead;
  private final boolean _isCompressed;
  private int _bytes;

  /**
   * @param monitor           ZkClient monitor for update the operation result.
   * @param startTimeMilliSec Operation initialization time.
   * @param bytes             The data size in bytes that is involved in the operation.
   * @param isRead            True if the operation is readonly.
   * @param isCompressed      True if the data is compressed.
   */
  public ZkAsyncCallMonitorContext(final ZkClientMonitor monitor, long startTimeMilliSec, int bytes,
      boolean isRead, boolean isCompressed) {
    _monitor = monitor;
    _startTimeMilliSec = startTimeMilliSec;
    _bytes = bytes;
    _isRead = isRead;
    _isCompressed = isCompressed;
  }

  public ZkAsyncCallMonitorContext(final ZkClientMonitor monitor, long startTimeMilliSec, int bytes,
      boolean isRead) {
    this(monitor, startTimeMilliSec, bytes, isRead, false);
  }


  /**
   * Update the operated data size in bytes.
   * @param bytes
   */
  void setBytes(int bytes) {
    _bytes = bytes;
  }

  /**
   * Record the operation result into the specified ZkClient monitor.
   * @param path
   */
  void recordAccess(String path) {
    if (_monitor != null) {
      if (_isRead) {
        _monitor.recordAsync(path, _bytes, _startTimeMilliSec, ZkClientMonitor.AccessType.READ);
      } else {
        _monitor.recordAsync(path, _bytes, _startTimeMilliSec, ZkClientMonitor.AccessType.WRITE);
        if (_isCompressed) {
          _monitor.increaseZnodeCompressCounter();
        }
      }
    }
  }

  /**
   * Record the operation failure into the specified ZkClient monitor.
   * @param path The monitored path
   */
  void recordFailure(String path) {
    if (_monitor != null) {
      if (_isRead) {
        _monitor.recordAsyncFailure(path, ZkClientMonitor.AccessType.READ);
      } else {
        _monitor.recordAsyncFailure(path, ZkClientMonitor.AccessType.WRITE);
      }
    }
  }
}
