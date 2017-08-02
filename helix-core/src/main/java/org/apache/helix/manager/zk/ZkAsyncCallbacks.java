package org.apache.helix.manager.zk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZkAsyncCallbacks {
  private static Logger LOG = Logger.getLogger(ZkAsyncCallbacks.class);

  public static class GetDataCallbackHandler extends DefaultCallback implements DataCallback {
    byte[] _data;
    Stat _stat;

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      if (rc == 0) {
        _data = data;
        _stat = stat;
        // update ctx with data size
        if (_data != null && ctx != null && ctx instanceof ZkAsyncCallContext) {
          ZkAsyncCallContext zkCtx = (ZkAsyncCallContext) ctx;
          zkCtx._bytes = _data.length;
        }
      }
      callback(rc, path, ctx);
    }
  }

  public static class SetDataCallbackHandler extends DefaultCallback implements StatCallback {
    Stat _stat;

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc == 0) {
        _stat = stat;
      }
      callback(rc, path, ctx);
    }

    public Stat getStat() {
      return _stat;
    }
  }

  public static class ExistsCallbackHandler extends DefaultCallback implements StatCallback {
    Stat _stat;

    @Override
    public void handle() {
      // TODO Auto-generated method stub
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc == 0) {
        _stat = stat;
      }
      callback(rc, path, ctx);
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

  /**
   * Default callback for zookeeper async api
   */
  public static abstract class DefaultCallback {
    AtomicBoolean _lock = new AtomicBoolean(false);
    int _rc = -1;

    public void callback(int rc, String path, Object ctx) {
      if (rc != 0 && LOG.isDebugEnabled()) {
        LOG.debug(this + ", rc:" + Code.get(rc) + ", path: " + path);
      }

      if (ctx != null && ctx instanceof ZkAsyncCallContext) {
        ZkAsyncCallContext zkCtx = (ZkAsyncCallContext) ctx;
        if (zkCtx._isRead) {
          zkCtx._monitor.recordRead(path, zkCtx._bytes, zkCtx._startTimeMilliSec);
        } else {
          zkCtx._monitor.recordWrite(path, zkCtx._bytes, zkCtx._startTimeMilliSec);
        }
      }

      _rc = rc;
      handle();

      synchronized (_lock) {
        _lock.set(true);
        _lock.notify();
      }
    }

    public boolean waitForSuccess() {
      try {
        synchronized (_lock) {
          while (!_lock.get()) {
            _lock.wait();
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

    abstract public void handle();
  }

  public static class ZkAsyncCallContext {
    private long _startTimeMilliSec;
    private int _bytes;
    private ZkClientMonitor _monitor;
    private boolean _isRead;

    public ZkAsyncCallContext(final ZkClientMonitor monitor, long startTimeMilliSec, int bytes,
        boolean isRead) {
      if (monitor == null) {
        throw new NullPointerException("ZkClientMonitor must not be null.");
      }
      _monitor = monitor;
      _startTimeMilliSec = startTimeMilliSec;
      _bytes = bytes;
      _isRead = isRead;
    }
  }

}
