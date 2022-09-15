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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// returns a thread pool object given a HelixManager identifier
public class CallbackEventThreadPoolFactory {
  private static Logger logger = LoggerFactory.getLogger(CallbackHandler.class);
  private static final int CALLBACK_EVENT_THREAD_POOL_SIZE = 10;
  private static final int CALLBACK_EVENT_THREAD_POOL_TTL_MINUTE = 3;
  static private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  static private Map<Integer, ThreadPoolExecutor> _managerToCallBackThreadPoolMap = new HashMap();
  static private Map<Integer, AtomicInteger> _callBackEventProcessorCountPerThreadPool =
      new HashMap();

  public static ThreadPoolExecutor getOrCreateThreadPool(int hash) {
    // should not use general lock for read
    _lock.readLock().lock();
    ThreadPoolExecutor result = null;
    if (_managerToCallBackThreadPoolMap.containsKey(hash)) {
      result = _managerToCallBackThreadPoolMap.get(hash);
      _callBackEventProcessorCountPerThreadPool.get(hash).incrementAndGet();
    }
    _lock.readLock().unlock();
    if (result == null) {
      result = getOrCreateThreadPoolHelper(hash);
    }
    return result;
  }

  private static ThreadPoolExecutor getOrCreateThreadPoolHelper(int hash) {
    ThreadPoolExecutor result = null;
    _lock.writeLock().lock();
    // first check if the key is already in the map
    if (_managerToCallBackThreadPoolMap.containsKey(hash)) {
      // downgrade to read lock since we dont need to modify the map
      _lock.readLock().lock();
      _lock.writeLock().unlock();
      result = _managerToCallBackThreadPoolMap.get(hash);
      _lock.readLock().unlock();
    } else {
      try {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat(String.format("CallbackHandlerExecutorService - %s ", hash)).build();
        result =
            new ThreadPoolExecutor(CALLBACK_EVENT_THREAD_POOL_SIZE, CALLBACK_EVENT_THREAD_POOL_SIZE,
                CALLBACK_EVENT_THREAD_POOL_TTL_MINUTE, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), namedThreadFactory);
        result.allowCoreThreadTimeOut(true);
        _managerToCallBackThreadPoolMap.put(hash, result);
        _callBackEventProcessorCountPerThreadPool.put(hash, new AtomicInteger(1));
      } finally {
        _lock.writeLock().unlock();
      }
    }

    return result;
  }

  public static void unregisterEventProcessor(int managerHash) {
    ExecutorService threadPoolToClose = null;
    _lock.writeLock().lock();
    if (_callBackEventProcessorCountPerThreadPool.get(managerHash).decrementAndGet() == 0) {
      _callBackEventProcessorCountPerThreadPool.remove(managerHash);
      threadPoolToClose = _managerToCallBackThreadPoolMap.remove(managerHash);
    }
    _lock.writeLock().unlock();

    if (threadPoolToClose != null) {
      threadPoolToClose.shutdown();
    }
  }
}