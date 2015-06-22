package org.apache.helix.taskexecution;

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

import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = {
    "ONLINE", "ERROR"
})
public class TaskStateModel extends TransitionHandler {
  private static Logger LOG = Logger.getLogger(TaskStateModel.class);

  private final String _workerId;
  private final String _partition;

  private final TaskFactory _taskFactory;

  private TaskResultStore _taskResultStore;

  public TaskStateModel(String workerId, String partition, TaskFactory taskFactory,
      TaskResultStore taskResultStore) {
    _partition = partition;
    _workerId = workerId;
    _taskFactory = taskFactory;
    _taskResultStore = taskResultStore;
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context)
      throws Exception {
    LOG.debug(_workerId + " becomes ONLINE from OFFLINE for " + _partition);
    ConfigAccessor clusterConfig = context.getManager().getConfigAccessor();
    HelixManager manager = context.getManager();
    HelixConfigScope clusterScope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(
            manager.getClusterName()).build();
    String json = clusterConfig.get(clusterScope, message.getResourceId().stringify());
    Dag.Node node = Dag.Node.fromJson(json);
    Set<String> parentIds = node.getParentIds();
    ResourceId resourceId = message.getResourceId();
    int numPartitions = node.getNumPartitions();
    Task task =
        _taskFactory.createTask(resourceId.stringify(), parentIds, manager, _taskResultStore);
    manager.addExternalViewChangeListener(task);

    LOG.debug("Starting task for " + _partition + "...");
    int partitionNum = Integer.parseInt(_partition.split("_")[1]);
    task.execute(resourceId.stringify(), numPartitions, partitionNum);
    LOG.debug("Task for " + _partition + " done");
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.debug(_workerId + " becomes OFFLINE from ONLINE for " + _partition);

  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.debug(_workerId + " becomes DROPPED from OFFLINE for " + _partition);
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    LOG.debug(_workerId + " becomes OFFLINE from ERROR for " + _partition);
  }

  @Override
  public void reset() {
    LOG.warn("Default reset() invoked");
  }
}
