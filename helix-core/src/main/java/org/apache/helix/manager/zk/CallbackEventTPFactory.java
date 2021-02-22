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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

// returns a thread pool object given a HelixManager identifier
public class CallbackEventTPFactory {
  static private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  static private Map<Integer, ThreadPoolExecutor> _managerToCallBackThreadPoolMap = new HashMap();
  static private Map<Integer, AtomicInteger> _callBackEventProcessorCountPerThreadPool =
      new HashMap();

  public static ThreadPoolExecutor getOrCreateThreadPool(int managerHash) {
    // should not use general lock for read
    _lock.readLock().lock();
    ThreadPoolExecutor result = null;
    if (_managerToCallBackThreadPoolMap.containsKey(managerHash)) {
      result = _managerToCallBackThreadPoolMap.get(managerHash);
      _callBackEventProcessorCountPerThreadPool.get(managerHash).incrementAndGet();
    }
    _lock.readLock().unlock();
    if (result == null) {
      result = getOrCreateThreadPoolHelper(managerHash);
    }
    return result;
  }

  private static ThreadPoolExecutor getOrCreateThreadPoolHelper(int managerHash) {
    ThreadPoolExecutor result;
    _lock.writeLock().lock();
    // first check if the key is already in the map
    if (_managerToCallBackThreadPoolMap.containsKey(managerHash)) {
      // downgrade to read lock since we dont need to modify the map
      _lock.readLock().lock();
      result = _managerToCallBackThreadPoolMap.get(managerHash);
      _lock.readLock().unlock();
    } else {
      ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
          .setNameFormat(String.format("CallbackHandlerExecutorService - %s ", managerHash))
          .build();
      result = new ThreadPoolExecutor(5, 5, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
          namedThreadFactory);
      result.allowCoreThreadTimeOut(true);
      _managerToCallBackThreadPoolMap.put(managerHash, result);
      _callBackEventProcessorCountPerThreadPool.put(managerHash, new AtomicInteger(0));
    }
    _lock.writeLock().unlock();
    return result;
  }

  public static void unregisterEventProcessor(int managerHash) {
    ThreadPoolExecutor threadPoolToClose = null;
    _lock.writeLock().lock();
    int newVal = _callBackEventProcessorCountPerThreadPool.get(managerHash).decrementAndGet();
    if (newVal == 0) {
      _callBackEventProcessorCountPerThreadPool.remove(managerHash);
      threadPoolToClose = _managerToCallBackThreadPoolMap.get(managerHash);
      _managerToCallBackThreadPoolMap.remove(managerHash);
    }
    _lock.writeLock().unlock();

    if (threadPoolToClose != null) {
      threadPoolToClose.shutdown();
    }
  }
}