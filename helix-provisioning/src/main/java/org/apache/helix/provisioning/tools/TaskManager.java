package org.apache.helix.provisioning.tools;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixRole;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.manager.zk.HelixConnectionAdaptor;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class TaskManager {
  private static final Logger LOG = Logger.getLogger(TaskManager.class);

  private final ClusterId _clusterId;
  private final HelixConnection _connection;
  private final TaskDriver _driver;

  public TaskManager(final ClusterId clusterId, final HelixConnection connection) {
    HelixRole dummyRole = new HelixRole() {
      @Override
      public HelixConnection getConnection() {
        return connection;
      }

      @Override
      public ClusterId getClusterId() {
        return clusterId;
      }

      @Override
      public Id getId() {
        return clusterId;
      }

      @Override
      public InstanceType getType() {
        return InstanceType.ADMINISTRATOR;
      }

      @Override
      public ClusterMessagingService getMessagingService() {
        return null;
      }
    };
    _driver = new TaskDriver(new HelixConnectionAdaptor(dummyRole));
    _clusterId = clusterId;
    _connection = connection;
  }

  public boolean createTaskQueue(String queueName, boolean isParallel) {
    Workflow.Builder builder = new Workflow.Builder(queueName);
    builder.addConfig(queueName, TaskConfig.COMMAND, queueName);
    builder.addConfig(queueName, TaskConfig.TARGET_PARTITIONS, "");
    builder.addConfig(queueName, TaskConfig.COMMAND_CONFIG, "");
    builder.addConfig(queueName, TaskConfig.LONG_LIVED + "", String.valueOf(true));
    if (isParallel) {
      builder
          .addConfig(queueName, TaskConfig.NUM_CONCURRENT_TASKS_PER_INSTANCE, String.valueOf(10));
    }
    Workflow workflow = builder.build();
    try {
      _driver.start(workflow);
    } catch (Exception e) {
      LOG.error("Failed to start queue " + queueName, e);
      return false;
    }
    return true;
  }

  public void addTaskToQueue(final String taskName, final String queueName) {
    HelixDataAccessor accessor = _connection.createDataAccessor(_clusterId);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    final ResourceId resourceId = ResourceId.from(queueName + "_" + queueName);
    String configPath = keyBuilder.resourceConfig(resourceId.toString()).getPath();
    DataUpdater<ZNRecord> dataUpdater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        // Update the partition integers to add one to the end, and have that integer map to the
        // task name
        String current = currentData.getSimpleField(TaskConfig.TARGET_PARTITIONS);
        int currentId = 0;
        if (current == null || current.isEmpty()) {
          currentData.setSimpleField(TaskConfig.TARGET_PARTITIONS, String.valueOf(currentId));
        } else {
          String[] parts = current.split(",");
          currentId = parts.length;
          currentData.setSimpleField(TaskConfig.TARGET_PARTITIONS, current + "," + currentId);
        }
        Map<String, String> partitionMap = currentData.getMapField(TaskConfig.TASK_NAME_MAP);
        if (partitionMap == null) {
          partitionMap = Maps.newHashMap();
          currentData.setMapField(TaskConfig.TASK_NAME_MAP, partitionMap);
        }
        partitionMap.put(resourceId.toString() + '_' + currentId, taskName);
        return currentData;
      }
    };
    List<DataUpdater<ZNRecord>> dataUpdaters = new ArrayList<DataUpdater<ZNRecord>>();
    dataUpdaters.add(dataUpdater);
    accessor.updateChildren(Arrays.asList(configPath), dataUpdaters, AccessOption.PERSISTENT);

    // Update the ideal state to trigger a change event
    DataUpdater<ZNRecord> noOpUpdater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        return currentData;
      }
    };
    String idealStatePath = keyBuilder.idealStates(queueName + "_" + queueName).getPath();
    dataUpdaters.clear();
    dataUpdaters.add(noOpUpdater);
    accessor.updateChildren(Arrays.asList(idealStatePath), dataUpdaters, AccessOption.PERSISTENT);
  }

  public void shutdownQueue(String queueName) {
  }
}
