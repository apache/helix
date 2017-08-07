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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordDelta;
import org.apache.helix.ZNRecordDelta.MergeOperation;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.StatusUpdate;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

public class ExternalViewComputeStage extends AbstractBaseStage {
  private static Logger LOG = Logger.getLogger(ExternalViewComputeStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    LOG.info("START ExternalViewComputeStage.process()");

    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());

    if (manager == null || resourceMap == null || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires ClusterManager|RESOURCES|DataCache");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());

    List<ExternalView> newExtViews = new ArrayList<ExternalView>();

    Map<String, ExternalView> curExtViews =
        dataAccessor.getChildValuesMap(keyBuilder.externalViews());

    for (String resourceName : resourceMap.keySet()) {
      ExternalView view = new ExternalView(resourceName);
      // view.setBucketSize(currentStateOutput.getBucketSize(resourceName));
      // if resource ideal state has bucket size, set it
      // otherwise resource has been dropped, use bucket size from current state instead
      Resource resource = resourceMap.get(resourceName);
      if (resource.getBucketSize() > 0) {
        view.setBucketSize(resource.getBucketSize());
      } else {
        view.setBucketSize(currentStateOutput.getBucketSize(resourceName));
      }

      for (Partition partition : resource.getPartitions()) {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceName, partition);
        if (currentStateMap != null && currentStateMap.size() > 0) {
          // Set<String> disabledInstances
          // = cache.getDisabledInstancesForResource(resource.toString());
          for (String instance : currentStateMap.keySet()) {
            // if (!disabledInstances.contains(instance))
            // {
            view.setState(partition.getPartitionName(), instance, currentStateMap.get(instance));
            // }
          }
        }
      }
      // Update cluster status monitor mbean
      ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
      IdealState idealState = cache.getIdealState(resourceName);
      ResourceConfig resourceConfig = cache.getResourceConfig(resourceName);
      if (idealState != null && (resourceConfig == null || !resourceConfig
          .isMonitoringDisabled())) {
        if (clusterStatusMonitor != null && !idealState.getStateModelDefRef()
            .equalsIgnoreCase(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
          StateModelDefinition stateModelDef =
              cache.getStateModelDef(idealState.getStateModelDefRef());
          clusterStatusMonitor
              .setResourceStatus(view, cache.getIdealState(view.getResourceName()),
                  stateModelDef);
        }
      } else {
        // Drop the metrics if the resource is dropped, or the MonitorDisabled is changed to true.
        clusterStatusMonitor.unregisterResource(view.getResourceName());
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
        // scheduler message, and then remove the partitions from the ideal state
        if (idealState != null
            && idealState.getStateModelDefRef().equalsIgnoreCase(
            DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
          updateScheduledTaskStatus(view, manager, idealState);
        }
      }
    }
    // TODO: consider not setting the externalview of SCHEDULER_TASK_QUEUE at all.
    // Are there any entity that will be interested in its change?

    // For the resource with DisableExternalView option turned on in IdealState
    // We will not actually create or write the externalView to ZooKeeper.
    List<PropertyKey> keys = new ArrayList<PropertyKey>();
    for(Iterator<ExternalView> it = newExtViews.iterator(); it.hasNext(); ) {
      ExternalView view = it.next();
      String resourceName = view.getResourceName();
      IdealState idealState = cache.getIdealState(resourceName);
      if (idealState != null && idealState.isExternalViewDisabled()) {
        it.remove();
        // remove the external view if the external view exists
        if (curExtViews.containsKey(resourceName)) {
          LOG.info("Remove externalView for resource: " + resourceName);
          dataAccessor.removeProperty(keyBuilder.externalView(resourceName));
        }
      } else {
        keys.add(keyBuilder.externalView(resourceName));
      }
    }

    // add/update external-views
    if (newExtViews.size() > 0) {
      dataAccessor.setChildren(keys, newExtViews);
    }

    // remove dead external-views
    for (String resourceName : curExtViews.keySet()) {
      if (!resourceMap.keySet().contains(resourceName)) {
        LOG.info("Remove externalView for resource: " + resourceName);
        dataAccessor.removeProperty(keyBuilder.externalView(resourceName));
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END ExternalViewComputeStage.process() for cluster " + cache.getClusterName()
        + ". took: " + (endTime - startTime) + " ms");
  }

  private void updateScheduledTaskStatus(ExternalView ev, HelixManager manager,
      IdealState taskQueueIdealState) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ZNRecord finishedTasks = new ZNRecord(ev.getResourceName());

    // Place holder for finished partitions
    Map<String, String> emptyMap = new HashMap<String, String>();
    List<String> emptyList = new LinkedList<String>();

    Map<String, Integer> controllerMsgIdCountMap = new HashMap<String, Integer>();
    Map<String, Map<String, String>> controllerMsgUpdates =
        new HashMap<String, Map<String, String>>();

    Builder keyBuilder = accessor.keyBuilder();

    for (String taskPartitionName : ev.getPartitionSet()) {
      for (String taskState : ev.getStateMap(taskPartitionName).values()) {
        if (taskState.equalsIgnoreCase(HelixDefinedState.ERROR.toString())
            || taskState.equalsIgnoreCase("COMPLETED")) {
          LOG.info(taskPartitionName + " finished as " + taskState);
          finishedTasks.getListFields().put(taskPartitionName, emptyList);
          finishedTasks.getMapFields().put(taskPartitionName, emptyMap);

          // Update original scheduler message status update
          if (taskQueueIdealState.getRecord().getMapField(taskPartitionName) != null) {
            String controllerMsgId =
                taskQueueIdealState.getRecord().getMapField(taskPartitionName)
                    .get(DefaultSchedulerMessageHandlerFactory.CONTROLLER_MSG_ID);
            if (controllerMsgId != null) {
              LOG.info(taskPartitionName + " finished with controllerMsg " + controllerMsgId);
              if (!controllerMsgUpdates.containsKey(controllerMsgId)) {
                controllerMsgUpdates.put(controllerMsgId, new HashMap<String, String>());
              }
              controllerMsgUpdates.get(controllerMsgId).put(taskPartitionName, taskState);
            }
          }
        }
      }
    }
    // fill the controllerMsgIdCountMap
    for (String taskId : taskQueueIdealState.getPartitionSet()) {
      String controllerMsgId =
          taskQueueIdealState.getRecord().getMapField(taskId)
              .get(DefaultSchedulerMessageHandlerFactory.CONTROLLER_MSG_ID);
      if (controllerMsgId != null) {
        if (!controllerMsgIdCountMap.containsKey(controllerMsgId)) {
          controllerMsgIdCountMap.put(controllerMsgId, 0);
        }
        controllerMsgIdCountMap.put(controllerMsgId,
            (controllerMsgIdCountMap.get(controllerMsgId) + 1));
      }
    }

    if (controllerMsgUpdates.size() > 0) {
      for (String controllerMsgId : controllerMsgUpdates.keySet()) {
        PropertyKey controllerStatusUpdateKey =
            keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.name(), controllerMsgId);
        StatusUpdate controllerStatusUpdate = accessor.getProperty(controllerStatusUpdateKey);
        for (String taskPartitionName : controllerMsgUpdates.get(controllerMsgId).keySet()) {
          Map<String, String> result = new HashMap<String, String>();
          result.put("Result", controllerMsgUpdates.get(controllerMsgId).get(taskPartitionName));
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
        // All done for the scheduled tasks that came from controllerMsgId, add summary for it
        Integer controllerMsgIdCount = controllerMsgIdCountMap.get(controllerMsgId);
        if (controllerMsgIdCount != null
            && controllerMsgUpdates.get(controllerMsgId).size() == controllerMsgIdCount.intValue()) {
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
          Map<String, String> summary = new TreeMap<String, String>();
          summary.put("TotalMessages:", "" + finishedTasksNum);
          summary.put("CompletedMessages", "" + completedTasksNum);

          controllerStatusUpdate.getRecord().setMapField("Summary", summary);
        }
        // Update the statusUpdate of controllerMsgId
        accessor.updateProperty(controllerStatusUpdateKey, controllerStatusUpdate);
      }
    }

    if (finishedTasks.getListFields().size() > 0) {
      ZNRecordDelta znDelta = new ZNRecordDelta(finishedTasks, MergeOperation.SUBTRACT);
      List<ZNRecordDelta> deltaList = new LinkedList<ZNRecordDelta>();
      deltaList.add(znDelta);
      IdealState delta = new IdealState(taskQueueIdealState.getResourceName());
      delta.setDeltaList(deltaList);

      // Remove the finished (COMPLETED or ERROR) tasks from the SCHEDULER_TASK_RESOURCE idealstate
      keyBuilder = accessor.keyBuilder();
      accessor.updateProperty(keyBuilder.idealStates(taskQueueIdealState.getResourceName()), delta);
    }
  }

}
