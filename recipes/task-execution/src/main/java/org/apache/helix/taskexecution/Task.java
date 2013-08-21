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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;

public abstract class Task implements ExternalViewChangeListener {
  CountDownLatch parentDependencyLatch;
  Set<String> completedParentTasks;

  private static final int TIMEOUT_SECONDS = 5;
  protected final Set<String> parentIds;
  protected final String id;
  private final HelixManager helixManager;
  protected final TaskResultStore resultStore;

  public Task(String id, Set<String> parentIds, HelixManager helixManager,
      TaskResultStore resultStore) {
    this.id = id;
    this.parentIds = parentIds;
    this.helixManager = helixManager;
    this.resultStore = resultStore;
    parentDependencyLatch = new CountDownLatch(1);
    completedParentTasks = new HashSet<String>();
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {

    if (areParentTasksDone(externalViewList)) {
      parentDependencyLatch.countDown();
    }
  }

  private boolean areParentTasksDone(List<ExternalView> externalViewList) {
    if (parentIds == null || parentIds.size() == 0) {
      return true;
    }

    for (ExternalView ev : externalViewList) {
      String resourceName = ev.getResourceName();
      if (parentIds.contains(resourceName)) {
        if (isParentTaskDone(ev)) {
          completedParentTasks.add(resourceName);
        }
      }
    }

    return parentIds.equals(completedParentTasks);
  }

  private boolean isParentTaskDone(ExternalView ev) {
    Set<String> partitionSet = ev.getPartitionSet();
    if (partitionSet.isEmpty()) {
      return false;
    }

    for (String partition : partitionSet) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      for (String instance : stateMap.keySet()) {
        if (!stateMap.get(instance).equalsIgnoreCase("Online")) {
          return false;
        }
      }
    }
    return true;
  }

  private List<ExternalView> getExternalViews() {
    String clusterName = helixManager.getClusterName();
    List<ExternalView> externalViewList = new ArrayList<ExternalView>();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(clusterName);
    for (String resourceName : resourcesInCluster) {
      ExternalView ev =
          helixManager.getClusterManagmentTool().getResourceExternalView(clusterName, resourceName);
      if (ev != null) {
        externalViewList.add(ev);
      }
    }

    return externalViewList;
  }

  public final void execute(String resourceName, int numPartitions, int partitionNum)
      throws Exception {
    if (!areParentTasksDone(getExternalViews())) {
      if (!parentDependencyLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        Set<String> pendingTasks = new HashSet<String>();
        pendingTasks.addAll(parentIds);
        pendingTasks.removeAll(completedParentTasks);
        throw new Exception(id
            + " timed out while waiting for the following parent tasks to finish : " + pendingTasks);
      }
    }

    executeImpl(resourceName, numPartitions, partitionNum);
  }

  protected abstract void executeImpl(String resourceName, int numPartitions, int partitionNum)
      throws Exception;

}
