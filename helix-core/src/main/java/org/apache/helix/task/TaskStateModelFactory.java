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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.JMException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.monitoring.mbeans.ThreadPoolExecutorMonitor;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class for {@link TaskStateModel}.
 */
public class TaskStateModelFactory extends StateModelFactory<TaskStateModel> {
  private static Logger LOG = LoggerFactory.getLogger(TaskStateModelFactory.class);

  // Unit in minutes. Need a retry timeout to prevent zkClient from hanging infinitely.
  private static final int ZKCLIENT_OPERATION_RETRY_TIMEOUT = 5;

  private final HelixManager _manager;
  private final Map<String, TaskFactory> _taskFactoryRegistry;
  private final ScheduledExecutorService _taskExecutor;
  private final ScheduledExecutorService _timerTaskExecutor;
  private ThreadPoolExecutorMonitor _monitor;

  public TaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
    this(manager, taskFactoryRegistry, createThreadPoolExecutor(manager));
  }

  // DO NOT USE! This size of provided thread pool will not be reflected to controller
  // properly, the controller may over schedule tasks to this participant. Task Framework needs to
  // have full control of the thread pool unlike the state transition thread pool.
  @Deprecated
  public TaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry,
      ScheduledExecutorService taskExecutor) {
    _manager = manager;
    _taskFactoryRegistry = taskFactoryRegistry;
    _taskExecutor = taskExecutor;
    // TODO: Hunter: I'm not sure why this needs to be a single thread executor. We could certainly
    // use more threads for timer tasks.
    _timerTaskExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "TaskStateModelFactory-timeTask_thread");
      }
    });
    if (_taskExecutor instanceof ThreadPoolExecutor) {
      try {
        _monitor = new ThreadPoolExecutorMonitor(TaskConstants.STATE_MODEL_NAME,
            (ThreadPoolExecutor) _taskExecutor);
      } catch (JMException e) {
        LOG.warn("Error in creating ThreadPoolExecutorMonitor for TaskStateModelFactory.", e);
      }
    }
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

  @VisibleForTesting
  void shutdownNow() {
    _taskExecutor.shutdownNow();
    _timerTaskExecutor.shutdownNow();
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

  /*
   * Create a RealmAwareZkClient to get thread pool sizes
   */
  protected static RealmAwareZkClient createZkClient(HelixManager manager) {
    if (!(manager instanceof ZKHelixManager)) {
      // TODO: None-ZKHelixManager cannot initialize this class. After interface rework of
      // HelixManager, the initialization should be allowed.
      throw new UnsupportedOperationException(
          "Only ZKHelixManager is supported for configurable thread pool.");
    }
    RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
        new RealmAwareZkClient.RealmAwareZkClientConfig().setZkSerializer(new ZNRecordSerializer());
    // Set operation retry timeout to prevent hanging infinitely
    clientConfig
        .setOperationRetryTimeout(Duration.ofMinutes(ZKCLIENT_OPERATION_RETRY_TIMEOUT).toMillis());
    String zkAddress = manager.getMetadataStoreConnectionString();

    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddress == null) {
      RealmAwareZkClient.RealmAwareZkConnectionConfig zkConnectionConfig =
          ((ZKHelixManager) manager).getRealmAwareZkConnectionConfig();
      // TODO: a fallback logic is created because it's possible for the ZKHelixManager to not
      // have a connection config, since a connection config may be created during
      // ZKHelixManager.connect(). This is the same problem as described earlier because connect()
      // may happen before or after TaskStateModelFactory initialization. Clean this up after that
      // problem is fixed.
      if (zkConnectionConfig == null) {
        String clusterName = manager.getClusterName();
        String shardingKey = HelixUtil.clusterNameToShardingKey(clusterName);
        zkConnectionConfig = new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
            .setZkRealmShardingKey(shardingKey).build();
      }
      try {
        return new FederatedZkClient(zkConnectionConfig, clientConfig);
      } catch (InvalidRoutingDataException | IllegalArgumentException e) {
        throw new HelixException("Failed to create FederatedZkClient!", e);
      }
    }

    // Note: operation retry timeout doesn't take effect due to github.com/apache/helix/issues/1682
    return SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
            clientConfig.createHelixZkClientConfig());
  }

  private static ScheduledExecutorService createThreadPoolExecutor(HelixManager manager) {
    // TODO: revisit the logic here - we are creating a connection although we already have a
    // manager. We cannot use the connection within manager because some users connect the manager
    // after registering the state model factory (in which case we cannot use manager's connection),
    // and some connect the manager before registering the state model factory (in which case we
    // can use manager's connection). We need to think about the right order and determine if we
    // want to enforce it, which may cause backward incompatibility.
    RealmAwareZkClient zkClient = createZkClient(manager);
    int targetThreadPoolSize;

    // Ensure the zkClient is closed after reading the pool size;
    try {
      targetThreadPoolSize = TaskUtil
          .getTargetThreadPoolSize(zkClient, manager.getClusterName(), manager.getInstanceName());
    } finally {
      zkClient.close();
    }

    LOG.info(
        "Obtained target thread pool size: {} from cluster {} for instance {}. Creating thread pool.",
        targetThreadPoolSize, manager.getClusterName(), manager.getInstanceName());
    return Executors.newScheduledThreadPool(targetThreadPoolSize, new ThreadFactory() {
      private AtomicInteger threadId = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "TaskStateModelFactory-task_thread-" + threadId.getAndIncrement());
      }
    });
  }
}
