package org.apache.helix.zookeeper.zkclient.callback;

import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;

public class ZkAsyncCallMonitorContext {
  private final long _startTimeMilliSec;
  private final ZkClientMonitor _monitor;
  private final boolean _isRead;
  private int _bytes;

  /**
   * @param monitor           ZkClient monitor for update the operation result.
   * @param startTimeMilliSec Operation initialization time.
   * @param bytes             The data size in bytes that is involved in the operation.
   * @param isRead            True if the operation is readonly.
   */
  public ZkAsyncCallMonitorContext(final ZkClientMonitor monitor, long startTimeMilliSec, int bytes,
      boolean isRead) {
    _monitor = monitor;
    _startTimeMilliSec = startTimeMilliSec;
    _bytes = bytes;
    _isRead = isRead;
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
        _monitor.record(path, _bytes, _startTimeMilliSec, ZkClientMonitor.AccessType.READ);
      } else {
        _monitor.record(path, _bytes, _startTimeMilliSec, ZkClientMonitor.AccessType.WRITE);
      }
    }
  }
}
