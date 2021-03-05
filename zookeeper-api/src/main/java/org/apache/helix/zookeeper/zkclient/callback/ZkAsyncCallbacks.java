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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkAsyncCallbacks {
  private static Logger LOG = LoggerFactory.getLogger(ZkAsyncCallbacks.class);
  public static final int UNKNOWN_RET_CODE = 255;

  public static class GetDataCallbackHandler extends DefaultCallback implements DataCallback {
    public byte[] _data;
    public Stat _stat;

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      if (rc == 0) {
        _data = data;
        _stat = stat;
        // update ctx with data size
        if (_data != null && ctx != null && ctx instanceof ZkAsyncCallMonitorContext) {
          ((ZkAsyncCallMonitorContext) ctx).setBytes(_data.length);
        }
      } else if(rc != Code.NONODE.intValue()) {
        if (ctx instanceof ZkAsyncCallMonitorContext) {
          ((ZkAsyncCallMonitorContext) ctx).recordFailure(path);
        }
      }
      callback(rc, path, ctx);
    }

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }
  }

  public static class SetDataCallbackHandler extends DefaultCallback implements StatCallback {
    Stat _stat;

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc == 0) {
        _stat = stat;
      } else {
        if (ctx instanceof ZkAsyncCallMonitorContext) {
          ((ZkAsyncCallMonitorContext) ctx).recordFailure(path);
        }
      }
      callback(rc, path, ctx);
    }

    public Stat getStat() {
      return _stat;
    }

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }
  }

  public static class ExistsCallbackHandler extends DefaultCallback implements StatCallback {
    public Stat _stat;

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc == 0) {
        _stat = stat;
      }
      callback(rc, path, ctx);
    }

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }
  }

  public static class CreateCallbackHandler extends DefaultCallback implements StringCallback {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      callback(rc, path, ctx);
    }

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }
  }

  public static class DeleteCallbackHandler extends DefaultCallback implements VoidCallback {
    @Override
    public void processResult(int rc, String path, Object ctx) {
      callback(rc, path, ctx);
    }

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }
  }

  public static class SyncCallbackHandler extends DefaultCallback implements AsyncCallback.VoidCallback {
    private String _sessionId;

    public SyncCallbackHandler(String sessionId) {
      _sessionId = sessionId;
    }

    @Override
    public void processResult(int rc, String path, Object ctx) {
      LOG.debug("sync() call with sessionID {} async return code: {}", _sessionId, rc);
      callback(rc, path, ctx);
    }

    @Override
    public void handle() {
      // Make compiler happy, not used.
    }

    @Override
    protected boolean needRetry(int rc) {
      try {
        switch (KeeperException.Code.get(rc)) {
          /** Connection to the server has been lost */
          case CONNECTIONLOSS:
            return true;
          default:
            return false;
        }
      } catch (ClassCastException | NullPointerException ex) {
        LOG.error("Session {} failed to handle unknown return code {}. Skip retrying. ex {}",
            _sessionId, rc, ex);
        return false;
      }
    }
  }
  /**
   * Default callback for zookeeper async api.
   */
  public static abstract class DefaultCallback implements CancellableZkAsyncCallback {
    AtomicBoolean _isOperationDone = new AtomicBoolean(false);
    int _rc = UNKNOWN_RET_CODE;

    public void callback(int rc, String path, Object ctx) {
      if (rc != 0 && LOG.isDebugEnabled()) {
        LOG.debug(this + ", rc:" + Code.get(rc) + ", path: " + path);
      }

      if (ctx != null && ctx instanceof ZkAsyncCallMonitorContext) {
        ((ZkAsyncCallMonitorContext) ctx).recordAccess(path);
      }

      _rc = rc;

      // If retry is requested by passing the retry callback context, do retry if necessary.
      if (needRetry(rc)) {
        if (ctx != null && ctx instanceof ZkAsyncRetryCallContext) {
          try {
            if (((ZkAsyncRetryCallContext) ctx).requestRetry()) {
              // The retry operation will be done asynchronously. Once it is done, the same callback
              // handler object shall be triggered to ensure the result is notified to the right
              // caller(s).
              return;
            } else {
              LOG.warn(
                  "Cannot request to retry the operation. The retry request thread may have been stopped.");
            }
          } catch (Throwable t) {
            LOG.error("Failed to request to retry the operation.", t);
          }
        } else {
          LOG.warn(
              "The provided callback context {} is not ZkAsyncRetryCallContext. Skip retrying.",
              ctx == null ? null : ctx.getClass().getName());
        }
      }

      // If operation is done successfully or no retry needed, notify the caller(s).
      try {
        handle();
      } finally {
        markOperationDone();
      }
    }

    public boolean isOperationDone() {
      return _isOperationDone.get();
    }

    /**
     * The blocking call that return true once the operation has been completed without retrying.
     */
    public boolean waitForSuccess() {
      try {
        synchronized (_isOperationDone) {
          while (!_isOperationDone.get()) {
            _isOperationDone.wait();
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted waiting for success", e);
      }
      return true;
    }

    public int getRc() {
      return _rc;
    }

    @Override
    public void notifyCallers() {
      LOG.warn("The callback {} has been cancelled.", this);
      markOperationDone();
    }

    /**
     * Additional callback handling.
     */
    abstract public void handle();

    private void markOperationDone() {
      synchronized (_isOperationDone) {
        _isOperationDone.set(true);
        _isOperationDone.notifyAll();
      }
    }

    /**
     * @param rc the return code
     * @return true if the error is transient and the operation may succeed when being retried.
     */
    protected boolean needRetry(int rc) {
      try {
        switch (Code.get(rc)) {
        /** Connection to the server has been lost */
        case CONNECTIONLOSS:
          /** The session has been expired by the server */
        case SESSIONEXPIRED:
          /** Session moved to another server, so operation is ignored */
        case SESSIONMOVED:
          return true;
        default:
          return false;
        }
      } catch (ClassCastException | NullPointerException ex) {
        LOG.error("Failed to handle unknown return code {}. Skip retrying.", rc, ex);
        return false;
      }
    }
  }

  @Deprecated
  public static class ZkAsyncCallContext extends ZkAsyncCallMonitorContext {
    ZkAsyncCallContext(ZkClientMonitor monitor, long startTimeMilliSec, int bytes, boolean isRead) {
      super(monitor, startTimeMilliSec, bytes, isRead);
    }
  }
}
