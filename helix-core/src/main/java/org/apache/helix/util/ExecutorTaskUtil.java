package org.apache.helix.util;

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

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorTaskUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorTaskUtil.class);

  /**
   * Wrap a callable so that any raised exception is logged
   * (can be interesting in case the callable is used as a completely asynchronous task
   * fed to an {@link java.util.concurrent.ExecutorService}), for which we are never
   * calling any of the {@link java.util.concurrent.Future#get()} or {@link java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)}
   * methods.
   */
  public static <T> Callable<T> wrap(Callable<T> callable) {
    return () -> {
      try {
        return callable.call();
      } catch (Throwable t) {
        LOG.error("Callable run on thread {} raised an exception and exited",
            Thread.currentThread().getName(), t);
        throw t;
      }
    };
  }

  /**
   * Wrap a runnable so that any raised exception is logged
   * (can be interesting in case the callable is used as a completely asynchronous task
   * fed to an {@link java.util.concurrent.ExecutorService}), for which we are never
   * calling any of the {@link java.util.concurrent.Future#get()} or {@link java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)}
   * methods.
   */
  public static Runnable wrap(Runnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable t) {
        LOG.error("Runnable run on thread {} raised an exception and exited",
            Thread.currentThread().getName(), t);
        throw t;
      }
    };
  }
}
