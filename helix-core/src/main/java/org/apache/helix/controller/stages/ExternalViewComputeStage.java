package org.apache.helix.controller.stages;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StatusUpdate;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalViewComputeStage extends AbstractAsyncBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(ExternalViewComputeStage.class);

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.ExternalViewComputeWorker;
  }

  @Override
  public void execute(final ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (manager == null || resourceMap == null || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires ClusterManager|RESOURCES|DataCache");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());

    List<ExternalView> newExtViews = new ArrayList<>();
    Set<String> monitoringResources = new HashSet<>();

    Map<String, ExternalView> curExtViews = cache.getExternalViews();

    for (Resource resource : resourceMap.values()) {
      try {
        computeExternalView(resource, currentStateOutput, cache, clusterStatusMonitor, curExtViews,
            manager, monitoringResources, newExtViews);
      } catch (HelixException ex) {
        LogUtil.logError(LOG, _eventId,
            "Failed to calculate external view for resource " + resource.getResourceName(), ex);
      }
    }

    // Keep MBeans for existing resources and unregister MBeans for dropped resources
    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.retainResourceMonitor(monitoringResources);
    }

    List<String> externalViewsToRemove = new ArrayList<>();
    // TODO: consider not setting the externalview of SCHEDULER_TASK_QUEUE at all.
    // Are there any entity that will be interested in its change?

    // For the resource with DisableExternalView option turned on in IdealState
    // We will not actually create or write the externalView to ZooKeeper.
    List<PropertyKey> keys = new ArrayList<>();
    for(Iterator<ExternalView> it = newExtViews.iterator(); it.hasNext(); ) {
      ExternalView view = it.next();
      String resourceName = view.getResourceName();
      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState != null && idealState.isExternalViewDisabled()) {
        it.remove();
        // remove the external view if the external view exists
        if (curExtViews.containsKey(resourceName)) {
          LogUtil
              .logInfo(LOG, _eventId, "Remove externalView for resource: " + resourceName);
          dataAccessor.removeProperty(keyBuilder.externalView(resourceName));
          externalViewsToRemove.add(resourceName);
        }
      } else {
        keys.add(keyBuilder.externalView(resourceName));
      }
    }

    // add/update external-views
    if (newExtViews.size() > 0) {
      dataAccessor.setChildren(keys, newExtViews);
      cache.updateExternalViews(newExtViews);
    }

    // remove dead external-views
    for (String resourceName : curExtViews.keySet()) {
      if (!resourceMap.keySet().contains(resourceName)) {
        LogUtil.logInfo(LOG, _eventId, "Remove externalView for resource: " + resourceName);
        dataAccessor.removeProperty(keyBuilder.externalView(resourceName));
        externalViewsToRemove.add(resourceName);
      }
    }
    cache.removeExternalViews(externalViewsToRemove);
  }

  private void computeExternalView(final Resource resource,
      final CurrentStateOutput currentStateOutput, final ResourceControllerDataProvider cache,
      final ClusterStatusMonitor clusterStatusMonitor, final Map<String, ExternalView> curExtViews,
      final HelixManager manager, Set<String> monitoringResources, List<ExternalView> newExtViews) {
    String resourceName = resource.getResourceName();
    ExternalView view = new ExternalView(resource.getResourceName());
    // if resource ideal state has bucket size, set it
    // otherwise resource has been dropped, use bucket size from current state instead
    if (resource.getBucketSize() > 0) {
      view.setBucketSize(resource.getBucketSize());
    } else {
      view.setBucketSize(currentStateOutput.getBucketSize(resourceName));
    }

    int totalPendingMessageCount = 0;

    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      if (currentStateMap != null && currentStateMap.size() > 0) {
        for (String instance : currentStateMap.keySet()) {
          view.setState(partition.getPartitionName(), instance, currentStateMap.get(instance));
        }
      }
      totalPendingMessageCount +=
          currentStateOutput.getPendingMessageMap(resource.getResourceName(), partition).size();
    }

    // Update cluster status monitor mbean
    IdealState idealState = cache.getIdealState(resourceName);
    ResourceConfig resourceConfig = cache.getResourceConfig(resourceName);
    if (clusterStatusMonitor != null) {
      if (idealState != null // has ideal state
          && (resourceConfig == null || !resourceConfig.isMonitoringDisabled()) // monitoring not disabled
          && !idealState.getStateModelDefRef() // and not a job resource
          .equalsIgnoreCase(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
        clusterStatusMonitor
            .setResourcePendingMessages(resourceName ,totalPendingMessageCount);
        monitoringResources.add(resourceName);
      }
    }

    ExternalView curExtView = curExtViews.get(resourceName);
    // copy simplefields from IS, in cases where IS is deleted copy it from existing ExternalView
    if (idealState != null) {
      view.getRecord().getSimpleFields().putAll(idealState.getRecord().getSimpleFields());
    } else if (curExtView != null) {
      view.getRecord().getSimpleFields().putAll(curExtView.getRecord().getSimpleFields());
    }

    // compare the new external view with current one, set only on different
    if (curExtView == null || !curExtView.getRecord().equals(view.getRecord())) {
      // Add external view to the list which will be written to ZK later.
      newExtViews.add(view);

      // For SCHEDULER_TASK_RESOURCE resource group (helix task queue), we need to find out which
      // task partitions are finished (COMPLETED or ERROR), update the status update of the original
      // scheduler message.
      if (idealState != null
          && idealState.getStateModelDefRef().equalsIgnoreCase(
          DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
        updateScheduledTaskStatus(view, manager, idealState);
      }
    }
  }

  private void updateScheduledTaskStatus(ExternalView ev, HelixManager manager,
      IdealState taskQueueIdealState) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    Map<String, Integer> controllerMsgIdCountMap = new HashMap<>();
    Map<String, Map<String, String>> controllerFinishedMsgs = new HashMap<>();

    Builder keyBuilder = accessor.keyBuilder();

    for (String taskPartitionName : ev.getPartitionSet()) {
      for (String taskState : ev.getStateMap(taskPartitionName).values()) {
        if (taskState.equalsIgnoreCase(HelixDefinedState.ERROR.toString()) || taskState
            .equalsIgnoreCase("COMPLETED")) {
          LogUtil.logInfo(LOG, _eventId, taskPartitionName + " finished as " + taskState);

          // Update original scheduler message status update
          Map<String, String> taskPartitionStatus = taskQueueIdealState.getRecord().getMapField(taskPartitionName);
          if (taskPartitionStatus != null) {
            String controllerMsgId = taskPartitionStatus.get(DefaultSchedulerMessageHandlerFactory.CONTROLLER_MSG_ID);
            if (controllerMsgId != null) {
              LogUtil.logInfo(LOG, _eventId, taskPartitionName + " finished with controllerMsg " + controllerMsgId);
              controllerFinishedMsgs.computeIfAbsent(controllerMsgId, id -> new HashMap<>())
                  .put(taskPartitionName, taskState);
            }
          }
        }
      }
    }

    // fill the controllerMsgIdCountMap
    for (Map<String, String> taskInfo : taskQueueIdealState.getRecord().getMapFields().values()) {
      String controllerMsgId = taskInfo.get(DefaultSchedulerMessageHandlerFactory.CONTROLLER_MSG_ID);
      if (controllerMsgId != null) {
        controllerMsgIdCountMap.put(controllerMsgId, controllerMsgIdCountMap.getOrDefault(controllerMsgId, 0) + 1);
      }
    }

    if (controllerFinishedMsgs.size() > 0) {
      for (String controllerMsgId : controllerFinishedMsgs.keySet()) {
        PropertyKey controllerStatusUpdateKey =
            keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.name(), controllerMsgId);
        StatusUpdate controllerStatusUpdate = accessor.getProperty(controllerStatusUpdateKey);

        Integer controllerMsgIdCount = controllerMsgIdCountMap.get(controllerMsgId);
        if (controllerMsgIdCount != null
            && controllerFinishedMsgs.get(controllerMsgId).size() == controllerMsgIdCount.intValue()) {
          // All done for the scheduled tasks that came from controllerMsgId, add summary for it
          for (String taskPartitionName : controllerFinishedMsgs.get(controllerMsgId).keySet()) {
            Map<String, String> result = new HashMap<>();
            result.put("Result", controllerFinishedMsgs.get(controllerMsgId).get(taskPartitionName));
            controllerStatusUpdate.getRecord().setMapField(
                "MessageResult "
                    + taskQueueIdealState.getRecord().getMapField(taskPartitionName)
                    .get(Message.Attributes.TGT_NAME.toString())
                    + " "
                    + taskPartitionName
                    + " "
                    + taskQueueIdealState.getRecord().getMapField(taskPartitionName)
                    .get(Message.Attributes.MSG_ID.toString()), result);
          }
          int finishedTasksNum = 0;
          int completedTasksNum = 0;
          for (String key : controllerStatusUpdate.getRecord().getMapFields().keySet()) {
            if (key.startsWith("MessageResult ")) {
              finishedTasksNum++;
            }
            if (controllerStatusUpdate.getRecord().getMapField(key).get("Result") != null) {
              if (controllerStatusUpdate.getRecord().getMapField(key).get("Result")
                  .equalsIgnoreCase("COMPLETED")) {
                completedTasksNum++;
              }
            }
          }
          Map<String, String> summary = new TreeMap<>();
          summary.put("TotalMessages:", "" + finishedTasksNum);
          summary.put("CompletedMessages", "" + completedTasksNum);

          controllerStatusUpdate.getRecord().setMapField("Summary", summary);
        }
        // Update the statusUpdate of controllerMsgId
        accessor.updateProperty(controllerStatusUpdateKey, controllerStatusUpdate);
      }
    }
  }
}
