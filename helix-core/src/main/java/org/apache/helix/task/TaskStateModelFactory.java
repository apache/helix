package org.apache.helix.task;

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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.JMException;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.monitoring.mbeans.ThreadPoolExecutorMonitor;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class for {@link TaskStateModel}.
 */
public class TaskStateModelFactory extends StateModelFactory<TaskStateModel> {
  private static Logger LOG = LoggerFactory.getLogger(TaskStateModelFactory.class);

  private final HelixManager _manager;
  private final Map<String, TaskFactory> _taskFactoryRegistry;
  private final ScheduledExecutorService _taskExecutor;
  private final ScheduledExecutorService _timerTaskExecutor;
  private ThreadPoolExecutorMonitor _monitor;

  public TaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
    _manager = manager;
    _taskFactoryRegistry = taskFactoryRegistry;
    // TODO: revisit the logic here - we are creating a connection although we already have a
    // manager. We cannot use the connection within manager because some users connect the manager
    // after registering the state model factory (in which case we cannot use manager's connection),
    // and some connect the manager before registering the state model factory (in which case we
    //can use manager's connection). We need to think about the right order and determine if we
    //want to enforce it, which may cause backward incompatibility.
    ConfigAccessor configAccessor = createConfigAccessor();
    int threadPoolSize = TaskUtil.getTargetThreadPoolSize(configAccessor, _manager.getClusterName(),
        _manager.getInstanceName());
    configAccessor.close();
    _taskExecutor = createTaskExecutor(threadPoolSize);
    _timerTaskExecutor = createTimerTaskExecutor();
    initializeTaskMonitor();
  }

  // FIXME: DO NOT USE! This size of provided thread pool will not be reflected to controller
  // properly, the controller may over schedule tasks to this participant. Task Framework needs to
  // have full control of the thread pool unlike the state transition thread pool.
  public TaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry,
      ScheduledExecutorService taskExecutor) {
    _manager = manager;
    _taskFactoryRegistry = taskFactoryRegistry;
    _taskExecutor = taskExecutor;
    _timerTaskExecutor = createTimerTaskExecutor();
    initializeTaskMonitor();
  }

  @Override
  public TaskStateModel createNewStateModel(String resourceName, String partitionKey) {
    return new TaskStateModel(_manager, _taskFactoryRegistry, _taskExecutor, _timerTaskExecutor);
  }

  public void shutdown() {
    if (_monitor != null) {
      _monitor.unregister();
    }
    _taskExecutor.shutdown();
    _timerTaskExecutor.shutdown();
    if (_monitor != null) {
      _monitor.unregister();
    }
  }

  public boolean isShutdown() {
    return _taskExecutor.isShutdown();
  }

  public boolean isTerminated() {
    return _taskExecutor.isTerminated();
  }

  private ScheduledExecutorService createTaskExecutor(int taskThreadPoolSize) {
    return Executors.newScheduledThreadPool(taskThreadPoolSize, new ThreadFactory() {
      private AtomicInteger threadId = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "TaskStateModelFactory-task_thread-" + threadId.getAndIncrement());
      }
    });
  }

  private ScheduledExecutorService createTimerTaskExecutor() {
    // TODO: Hunter: I'm not sure why this needs to be a single thread executor. We could certainly
    // use more threads for timer tasks.
    return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "TaskStateModelFactory-timeTask_thread");
      }
    });
  }

  private void initializeTaskMonitor() {
    if (_taskExecutor instanceof ThreadPoolExecutor) {
      try {
        _monitor = new ThreadPoolExecutorMonitor(TaskConstants.STATE_MODEL_NAME,
            (ThreadPoolExecutor) _taskExecutor);
      } catch (JMException e) {
        LOG.warn("Error in creating ThreadPoolExecutorMonitor for TaskStateModelFactory.", e);
      }
    }
  }

  /*
   * Create a config accessor to get the thread pool size
   */
  protected ConfigAccessor createConfigAccessor() {
    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED)) {
      String clusterName = _manager.getClusterName();
      String shardingKey = clusterName.charAt(0) == '/' ? clusterName : "/" + clusterName;
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
          new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
              .setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
              .setZkRealmShardingKey(shardingKey).build();
      return new ConfigAccessor.Builder().setRealmAwareZkConnectionConfig(connectionConfig).build();
    }

    return new ConfigAccessor.Builder().setZkAddress(_manager.getMetadataStoreConnectionString())
        .build();
  }
}
