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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkAsyncRetryThread extends Thread {
  private static Logger LOG = LoggerFactory.getLogger(ZkAsyncRetryThread.class);
  private BlockingQueue<ZkAsyncRetryCallContext> _retryContexts = new LinkedBlockingQueue<>();
  private volatile boolean _isReady = true;

  public ZkAsyncRetryThread(String name) {
    setDaemon(true);
    setName("ZkClient-AsyncCallback-Retry-" + getId() + "-" + name);
  }

  @Override
  public void run() {
    LOG.info("Starting ZkClient AsyncCallback retry thread.");
    try {
      while (!isInterrupted()) {
        ZkAsyncRetryCallContext context = _retryContexts.take();
        try {
          context.doRetry();
        } catch (InterruptedException | ZkInterruptedException e) {
          // if interrupted, stop retrying and interrupt the thread.
          context.cancel();
          interrupt();
        } catch (Throwable e) {
          LOG.error("Error retrying callback {}, cancelling it", context, e);
          // Cancel the context so the upstream caller can stop waiting
          context.cancel();
        }
      }
    } catch (InterruptedException e) {
      LOG.info("ZkClient AsyncCallback retry thread is interrupted.");
    }
    synchronized (this) {
      // Mark ready to be false, so no new requests will be sent.
      _isReady = false;
      // Notify to all the callers waiting for the result.
      for (ZkAsyncRetryCallContext context : _retryContexts) {
        context.cancel();
      }
    }
    LOG.info("Terminate ZkClient AsyncCallback retry thread.");
  }

  synchronized boolean sendRetryRequest(ZkAsyncRetryCallContext context) {
    if (_isReady) {
      _retryContexts.add(context);
      return true;
    }
    return false;
  }
}
