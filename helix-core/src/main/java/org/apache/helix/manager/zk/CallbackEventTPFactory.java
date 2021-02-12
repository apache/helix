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

// returns a thread pool object given a HelixManager identifier

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class CallbackEventTPFactory {
  static private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  static private Map<Integer, ThreadPoolExecutor> _managerToCallBackThreadPoolMap = new HashMap();

  public CallbackEventTPFactory() {}

  public static ThreadPoolExecutor getThreadPool(int mapHash) {
    // should not use general lock for read
    _lock.readLock().lock();
    ThreadPoolExecutor result;
    if (_managerToCallBackThreadPoolMap.containsKey(mapHash)) {
      result =  _managerToCallBackThreadPoolMap.get(mapHash);
      _lock.readLock().unlock();
    } else {
      _lock.readLock().unlock();
      result = getAndCreateThreadPool(mapHash);
    }
    return result;
  }

  public static ThreadPoolExecutor getAndCreateThreadPool(int mapHash) {
    ThreadPoolExecutor result;
    _lock.writeLock().lock();
    // first check if the key is already in the map
    if (_managerToCallBackThreadPoolMap.containsKey(mapHash)) {
      // downgrade to read lock
      _lock.readLock().lock();
      result = _managerToCallBackThreadPoolMap.get(mapHash);
      _lock.readLock().unlock();
    } else {
      ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(
          String.format("CallbackHandlerExecutorService - %s ", mapHash)).build();
      result = new ThreadPoolExecutor(5, 5, 1, TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(), namedThreadFactory);
      result.allowCoreThreadTimeOut(true);
      _managerToCallBackThreadPoolMap.put(mapHash, result);
    }
    _lock.writeLock().unlock();
    return result;
  }


}