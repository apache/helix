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
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRole;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.manager.zk.HelixConnectionAdaptor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class TaskManager {
  private static final Logger LOG = Logger.getLogger(TaskManager.class);

  private final ClusterId _clusterId;
  private final HelixConnection _connection;
  private final HelixManager _manager;
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
    _manager = new HelixConnectionAdaptor(dummyRole);
    _driver = new TaskDriver(_manager);
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
      builder.addConfig(queueName, TaskConfig.NUM_CONCURRENT_TASKS_PER_INSTANCE,
          String.valueOf(Integer.MAX_VALUE));
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
    // Update the resource config with the new partition count
    HelixDataAccessor accessor = _connection.createDataAccessor(_clusterId);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    final ResourceId resourceId = resourceId(queueName);
    final int[] numPartitions = {
      0
    };
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
          numPartitions[0] = currentId + 1;
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
    String configPath = keyBuilder.resourceConfig(resourceId.toString()).getPath();
    List<DataUpdater<ZNRecord>> dataUpdaters = new ArrayList<DataUpdater<ZNRecord>>();
    dataUpdaters.add(dataUpdater);
    accessor.updateChildren(Arrays.asList(configPath), dataUpdaters, AccessOption.PERSISTENT);

    // Update the ideal state with the proper partition count
    DataUpdater<ZNRecord> idealStateUpdater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        currentData.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(),
            String.valueOf(numPartitions[0]));
        return currentData;
      }
    };
    String idealStatePath = keyBuilder.idealStates(queueName + "_" + queueName).getPath();
    dataUpdaters.clear();
    dataUpdaters.add(idealStateUpdater);
    accessor.updateChildren(Arrays.asList(idealStatePath), dataUpdaters, AccessOption.PERSISTENT);
  }

  public void cancelTask(String queueName, String taskName) {
    // Get the mapped task name
    final ResourceId resourceId = resourceId(queueName);
    HelixDataAccessor accessor = _connection.createDataAccessor(_clusterId);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ResourceConfiguration resourceConfig =
        accessor.getProperty(keyBuilder.resourceConfig(resourceId.stringify()));
    if (resourceConfig == null) {
      LOG.error("Queue " + queueName + " does not exist!");
      return;
    }
    Map<String, String> taskMap = resourceConfig.getRecord().getMapField(TaskConfig.TASK_NAME_MAP);
    if (taskMap == null) {
      LOG.error("Task " + taskName + " in queue " + queueName + " does not exist!");
      return;
    }
    String partitionName = null;
    for (Map.Entry<String, String> e : taskMap.entrySet()) {
      String possiblePartition = e.getKey();
      String possibleTask = e.getValue();
      if (taskName.equals(possibleTask)) {
        partitionName = possiblePartition;
        break;
      }
    }
    if (partitionName == null) {
      LOG.error("Task " + taskName + " in queue " + queueName + " does not exist!");
      return;
    }

    // Now search the external view for who is running the task
    ExternalView externalView =
        accessor.getProperty(keyBuilder.externalView(resourceId.toString()));
    if (externalView == null) {
      LOG.error("Queue " + queueName + " was never started!");
      return;
    }
    PartitionId partitionId = PartitionId.from(partitionName);
    Map<ParticipantId, State> stateMap = externalView.getStateMap(partitionId);
    if (stateMap == null || stateMap.isEmpty()) {
      LOG.warn("Task " + taskName + " in queue " + queueName + " is not currently running");
      return;
    }
    ParticipantId targetParticipant = null;
    for (ParticipantId participantId : stateMap.keySet()) {
      targetParticipant = participantId;
    }
    if (targetParticipant == null) {
      LOG.warn("Task " + taskName + " in queue " + queueName + " is not currently running");
      return;
    }

    // Send a request to stop to the appropriate live instance
    LiveInstance liveInstance =
        accessor.getProperty(keyBuilder.liveInstance(targetParticipant.toString()));
    if (liveInstance == null) {
      LOG.error("Task " + taskName + " in queue " + queueName
          + " is assigned to a non-running participant");
      return;
    }
    SessionId sessionId = liveInstance.getTypedSessionId();
    TaskUtil.setRequestedState(accessor, targetParticipant.toString(), sessionId.toString(),
        resourceId.toString(), partitionId.toString(), TaskPartitionState.STOPPED);
    LOG.info("Task" + taskName + " for queue " + queueName + " instructed to stop");
  }

  public void shutdownQueue(String queueName) {
    // Check if tasks are complete, then set task and workflows to complete

    // Otherwise, send a stop for everybody
    _driver.stop(resourceId(queueName).toString());
  }

  private ResourceId resourceId(String queueName) {
    return ResourceId.from(queueName + '_' + queueName);
  }
}
