package org.apache.helix.messaging.handling;

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

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface TaskExecutor {
  public static final int DEFAULT_PARALLEL_TASKS = 40;

  /**
   * register message handler factory this executor can handle
   * @param type
   * @param factory
   */
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory);

  /**
   * register message handler factory this executor can handle with specified
   * thread-pool size
   * @param type
   * @param factory
   * @param threadpoolSize
   */
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory,
      int threadPoolSize);

  /**
   * schedule a message execution
   * @param message
   * @param handler
   * @param context
   */
  public boolean scheduleTask(MessageTask task);

  /**
   * blocking on scheduling all tasks
   * @param tasks
   */
  public List<Future<HelixTaskResult>> invokeAllTasks(List<MessageTask> tasks, long timeout,
      TimeUnit unit) throws InterruptedException;

  /**
   * cancel a message execution
   * @param message
   * @param context
   */
  public boolean cancelTask(MessageTask task);

  /**
   * cancel the timeout for the given task
   * @param task
   * @return
   */
  public boolean cancelTimeoutTask(MessageTask task);

  /**
   * finish a message execution
   * @param message
   */
  public void finishTask(MessageTask task);

  /**
   * shutdown executor
   */
  public void shutdown();
}
