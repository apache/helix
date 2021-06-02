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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.common.DedupEventBlockingQueue;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Each batch mode is enabled, each CallbackHandler has a CallbackEventTPExecutor.
 * It has a dedupe queue for pending call back event. Pending call back event will
 * be submitted to a thread pool one at a time when
 * 1. This is the first ever call back event for the callBackHandler, or
 * 2. The previous call back event handling process is finished in thread pool.
 */

public class CallbackEventExecutor {
  private static Logger logger = LoggerFactory.getLogger(CallbackHandler.class);

  private DedupEventBlockingQueue<NotificationContext.Type, NotificationContext>
      _callBackEventQueue;
  private final HelixManager _manager;
  private Future _futureCallBackProcessEvent = null;
  private ThreadPoolExecutor _threadPoolExecutor;
  private boolean _isShutdown = false;

  public CallbackEventExecutor(HelixManager manager) {
    _callBackEventQueue = new DedupEventBlockingQueue<>();
    _manager = manager;
    _threadPoolExecutor = CallbackEventThreadPoolFactory.getOrCreateThreadPool(manager.hashCode());
  }

  class CallbackProcessor implements Runnable {
    private final CallbackHandler _handler;
    protected final String _processorName;
    private final NotificationContext _event;

    public CallbackProcessor(CallbackHandler handler, NotificationContext event) {
      _processorName = _manager.getClusterName() + "-CallbackProcessor@" + Integer
          .toHexString(handler.hashCode());
      _handler = handler;
      _event = event;
    }

    @Override
    public void run() {
      try {
        _handler.invoke(_event);
      } catch (ZkInterruptedException e) {
        logger.warn(_processorName + " thread caught a ZK connection interrupt", e);
      } catch (ThreadDeath death) {
        logger.error(_processorName + " thread dead " + _processorName, death);
      } catch (Throwable t) {
        logger.error(_processorName + " thread failed while running " + _processorName, t);
      }
      submitPendingHandleCallBackEventToManagerThreadPool(_handler);
    }
  }

  public void submitEventToExecutor(NotificationContext.Type eventType, NotificationContext event,
      CallbackHandler handler) {
    synchronized (_callBackEventQueue) {
      if (_isShutdown) {
        logger.error("Failed to process callback. CallbackEventExecutor is already shut down.");
      }
      if (_futureCallBackProcessEvent == null || _futureCallBackProcessEvent.isDone()) {
        _futureCallBackProcessEvent =
            _threadPoolExecutor.submit(new CallbackProcessor(handler, event));
      } else {
        _callBackEventQueue.put(eventType, event);
      }
    }
  }

  private void submitPendingHandleCallBackEventToManagerThreadPool(CallbackHandler handler) {
    synchronized (_callBackEventQueue) {
      if (_callBackEventQueue.size() != 0) {
        try {
          NotificationContext event = _callBackEventQueue.take();
          _futureCallBackProcessEvent =
              _threadPoolExecutor.submit(new CallbackProcessor(handler, event));
        } catch (InterruptedException e) {
          logger
              .error("Error when submitting pending HandleCallBackEvent to manager thread pool", e);
        }
      }
    }
  }

  public void reset() {
    synchronized (_callBackEventQueue) {
      _callBackEventQueue.clear();
      if (_futureCallBackProcessEvent != null) {
        _futureCallBackProcessEvent.cancel(false);
      }
    }
  }

  public void unregisterFromFactory() {
    _isShutdown = true;
    reset();
    CallbackEventThreadPoolFactory.unregisterEventProcessor(_manager.hashCode());
    _threadPoolExecutor = null;
  }
}
