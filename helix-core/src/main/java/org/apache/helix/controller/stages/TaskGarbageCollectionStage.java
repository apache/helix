package org.apache.helix.controller.stages;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskGarbageCollectionStage extends AbstractAsyncBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(TaskGarbageCollectionStage.class);
  private static RebalanceScheduler _rebalanceScheduler = new RebalanceScheduler();

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.TaskJobPurgeWorker;
  }

  @Override
  public void process(ClusterEvent event) throws Exception {
    WorkflowControllerDataProvider dataProvider =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    event.addAttribute(AttributeName.WORKFLOW_CONFIG_MAP.name(),
        dataProvider.getWorkflowConfigMap());
    event.addAttribute(AttributeName.RESOURCE_CONTEXT_MAP.name(), dataProvider.getContexts());

    super.process(event);
  }

  @Override
  public void execute(ClusterEvent event) {
    Map<String, WorkflowConfig> workflowConfigMap =
        event.getAttribute(AttributeName.WORKFLOW_CONFIG_MAP.name());
    Map<String, ZNRecord> resourceContextMap =
        event.getAttribute(AttributeName.RESOURCE_CONTEXT_MAP.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());

    if (manager == null) {
      LOG.warn(
          " HelixManager is null for event {}({}) in cluster {}. Skip TaskGarbageCollectionStage.",
          event.getEventId(), event.getEventType(), event.getClusterName());
      return;
    }

    Set<WorkflowConfig> existingWorkflows = new HashSet<>(workflowConfigMap.values());
    for (WorkflowConfig workflowConfig : existingWorkflows) {
      // clean up the expired jobs if it is a queue.
      if (workflowConfig != null && (!workflowConfig.isTerminable() || workflowConfig
          .isJobQueue())) {
        String workflowId = workflowConfig.getWorkflowId();
        if (resourceContextMap.containsKey(workflowId)
            && resourceContextMap.get(workflowId) != null) {
          try {
            TaskUtil.purgeExpiredJobs(workflowId, workflowConfig,
                new WorkflowContext(resourceContextMap.get(workflowId)), manager,
                _rebalanceScheduler);
          } catch (Exception e) {
            LOG.warn(String.format("Failed to purge job for workflow %s with reason %s", workflowId,
                e.toString()));
          }
        } else {
          LOG.warn(String.format("Workflow %s context does not exist!", workflowId));
        }
      }
    }

    TaskUtil.workflowGarbageCollection(workflowConfigMap, resourceContextMap, manager);
  }
}
