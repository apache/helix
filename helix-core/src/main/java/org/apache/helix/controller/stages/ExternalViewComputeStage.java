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
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.SchedulerTaskConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
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

    HelixManager manager = event.getAttribute("helixmanager");
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    Cluster cluster = event.getAttribute("Cluster");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (manager == null || resourceMap == null || cluster == null || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires ClusterManager|RESOURCES|Cluster|ClusterDataCache");
    }

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    ResourceCurrentState currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());

    List<ExternalView> newExtViews = new ArrayList<ExternalView>();
    List<PropertyKey> keys = new ArrayList<PropertyKey>();

    // TODO use external-view accessor
    Map<String, ExternalView> curExtViews =
        dataAccessor.getChildValuesMap(keyBuilder.externalViews());

    for (ResourceId resourceId : resourceMap.keySet()) {
      ExternalView view = new ExternalView(resourceId.stringify());
      // view.setBucketSize(currentStateOutput.getBucketSize(resourceName));
      // if resource ideal state has bucket size, set it
      // otherwise resource has been dropped, use bucket size from current state instead
      ResourceConfig resource = resourceMap.get(resourceId);
      SchedulerTaskConfig schedulerTaskConfig = resource.getSchedulerTaskConfig();

      if (resource.getIdealState().getBucketSize() > 0) {
        view.setBucketSize(resource.getIdealState().getBucketSize());
      } else {
        view.setBucketSize(currentStateOutput.getBucketSize(resourceId));
      }
      for (PartitionId partitionId : resource.getSubUnitSet()) {
        Map<ParticipantId, State> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceId, partitionId);
        if (currentStateMap != null && currentStateMap.size() > 0) {
          // Set<String> disabledInstances
          // = cache.getDisabledInstancesForResource(resource.toString());
          for (ParticipantId participantId : currentStateMap.keySet()) {
            // if (!disabledInstances.contains(instance))
            // {
            view.setState(partitionId.stringify(), participantId.stringify(),
                currentStateMap.get(participantId).toString());
            // }
          }
        }
      }

      // Update cluster status monitor mbean
      ClusterStatusMonitor clusterStatusMonitor =
          (ClusterStatusMonitor) event.getAttribute("clusterStatusMonitor");
      IdealState idealState = cache._idealStateMap.get(view.getResourceName());
      if (idealState != null) {
        if (clusterStatusMonitor != null
            && !idealState.getStateModelDefRef().equalsIgnoreCase(
                DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
          StateModelDefinition stateModelDef =
              cache.getStateModelDef(idealState.getStateModelDefRef());
          clusterStatusMonitor.setResourceStatus(view,
              cache._idealStateMap.get(view.getResourceName()), stateModelDef);
        }
      } else {
        // Drop the metrics for the dropped resource
        clusterStatusMonitor.unregisterResource(view.getResourceName());
      }

      // compare the new external view with current one, set only on different
      ExternalView curExtView = curExtViews.get(resourceId.stringify());
      if (curExtView == null || !curExtView.getRecord().equals(view.getRecord())) {
        keys.add(keyBuilder.externalView(resourceId.stringify()));
        newExtViews.add(view);

        // For SCHEDULER_TASK_RESOURCE resource group (helix task queue), we need to find out which
        // task
        // partitions are finished (COMPLETED or ERROR), update the status update of the original
        // scheduler
        // message, and then remove the partitions from the ideal state
        if (idealState != null && idealState.getStateModelDefId() != null
            && idealState.getStateModelDefId().equalsIgnoreCase(StateModelDefId.SchedulerTaskQueue)) {
          updateScheduledTaskStatus(resourceId, view, manager, schedulerTaskConfig);
        }
      }
    }
    // TODO: consider not setting the externalview of SCHEDULER_TASK_QUEUE at all.
    // Are there any entity that will be interested in its change?

    // add/update external-views
    if (newExtViews.size() > 0) {
      dataAccessor.setChildren(keys, newExtViews);
    }

    // remove dead external-views
    for (String resourceName : curExtViews.keySet()) {
      if (!resourceMap.containsKey(ResourceId.from(resourceName))) {
        LOG.info("Remove externalView for resource: " + resourceName);
        dataAccessor.removeProperty(keyBuilder.externalView(resourceName));
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END ExternalViewComputeStage.process(). took: " + (endTime - startTime) + " ms");
  }

  // TODO fix it
  private void updateScheduledTaskStatus(ResourceId resourceId, ExternalView ev,
      HelixManager manager, SchedulerTaskConfig schedulerTaskConfig) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord finishedTasks = new ZNRecord(ev.getResourceName());

    // Place holder for finished partitions
    Map<String, String> emptyMap = new HashMap<String, String>();
    List<String> emptyList = new LinkedList<String>();

    Map<String, Integer> controllerMsgIdCountMap = new HashMap<String, Integer>();
    Map<String, Map<String, String>> controllerMsgUpdates =
        new HashMap<String, Map<String, String>>();

    for (String taskPartitionName : ev.getPartitionSet()) {
      for (String taskState : ev.getStateMap(taskPartitionName).values()) {
        if (taskState.equalsIgnoreCase(HelixDefinedState.ERROR.toString())
            || taskState.equalsIgnoreCase("COMPLETED")) {
          LOG.info(taskPartitionName + " finished as " + taskState);
          finishedTasks.setListField(taskPartitionName, emptyList);
          finishedTasks.setMapField(taskPartitionName, emptyMap);

          // Update original scheduler message status update
          Message innerMessage =
              schedulerTaskConfig.getInnerMessage(PartitionId.from(taskPartitionName));
          if (innerMessage != null) {
            String controllerMsgId = innerMessage.getControllerMessageId();
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
    for (PartitionId taskId : schedulerTaskConfig.getPartitionSet()) {
      Message innerMessage = schedulerTaskConfig.getInnerMessage(taskId);
      String controllerMsgId = innerMessage.getControllerMessageId();

      if (controllerMsgId != null) {
        Integer curCnt = controllerMsgIdCountMap.get(controllerMsgId);
        if (curCnt == null) {
          curCnt = 0;
        }
        controllerMsgIdCountMap.put(controllerMsgId, curCnt + 1);
      }
    }

    if (controllerMsgUpdates.size() > 0) {
      for (String controllerMsgId : controllerMsgUpdates.keySet()) {
        PropertyKey controllerStatusUpdateKey =
            keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), controllerMsgId);
        StatusUpdate controllerStatusUpdate = accessor.getProperty(controllerStatusUpdateKey);
        for (String taskPartitionName : controllerMsgUpdates.get(controllerMsgId).keySet()) {
          Message innerMessage =
              schedulerTaskConfig.getInnerMessage(PartitionId.from(taskPartitionName));

          Map<String, String> result = new HashMap<String, String>();
          result.put("Result", controllerMsgUpdates.get(controllerMsgId).get(taskPartitionName));
          controllerStatusUpdate.getRecord().setMapField(
              "MessageResult " + innerMessage.getTgtName() + " " + taskPartitionName + " "
                  + innerMessage.getMessageId(), result);
        }

        // All done for the scheduled tasks that came from controllerMsgId, add summary for it
        if (controllerMsgUpdates.get(controllerMsgId).size() == controllerMsgIdCountMap.get(
            controllerMsgId).intValue()) {
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
      IdealState delta = new IdealState(resourceId);
      delta.setDeltaList(deltaList);

      // Remove the finished (COMPLETED or ERROR) tasks from the SCHEDULER_TASK_RESOURCE idealstate
      keyBuilder = accessor.keyBuilder();
      accessor.updateProperty(keyBuilder.idealStates(resourceId.stringify()), delta);
    }
  }

}
