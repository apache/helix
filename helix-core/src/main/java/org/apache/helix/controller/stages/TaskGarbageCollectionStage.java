package org.apache.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
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
    // Use main thread to compute what jobs need to be purged, and what workflows need to be gc'ed.
    // This is to avoid race conditions since the cache will be modified. After this work, then the
    // async work will happen.
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      LOG.warn(
          "HelixManager is null for event {}({}) in cluster {}. Skip TaskGarbageCollectionStage.",
          event.getEventId(), event.getEventType(), event.getClusterName());
      return;
    }

    Map<String, Set<String>> expiredJobsMap = new HashMap<>();
    Set<String> workflowsToBeDeleted = new HashSet<>();
    WorkflowControllerDataProvider dataProvider =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    for (Map.Entry<String, ZNRecord> entry : dataProvider.getContexts().entrySet()) {
      WorkflowConfig workflowConfig = dataProvider.getWorkflowConfig(entry.getKey());
      if (workflowConfig != null && (!workflowConfig.isTerminable() || workflowConfig
          .isJobQueue())) {
        WorkflowContext workflowContext = dataProvider.getWorkflowContext(entry.getKey());
        long purgeInterval = workflowConfig.getJobPurgeInterval();
        long currentTime = System.currentTimeMillis();
        if (purgeInterval > 0
            && workflowContext.getLastJobPurgeTime() + purgeInterval <= currentTime) {
          // Find jobs that are ready to be purged
          Set<String> expiredJobs =
              TaskUtil.getExpiredJobsFromCache(dataProvider, workflowConfig, workflowContext);
          if (!expiredJobs.isEmpty()) {
            expiredJobsMap.put(workflowConfig.getWorkflowId(), expiredJobs);
          }
          scheduleNextJobPurge(workflowConfig.getWorkflowId(), currentTime, purgeInterval,
              _rebalanceScheduler, manager);
        }
      } else if (workflowConfig == null && entry.getValue() != null && entry.getValue().getId()
          .equals(TaskUtil.WORKFLOW_CONTEXT_KW)) {
        // Find workflows that need to be purged
        workflowsToBeDeleted.add(entry.getKey());
      }
    }
    event.addAttribute(AttributeName.EXPIRED_JOBS_MAP.name(),
        Collections.unmodifiableMap(expiredJobsMap));
    event.addAttribute(AttributeName.WORKFLOWS_TO_BE_DELETED.name(),
        Collections.unmodifiableSet(workflowsToBeDeleted));

    super.process(event);
  }

  @Override
  public void execute(ClusterEvent event) {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      LOG.warn(
          "HelixManager is null for event {}({}) in cluster {}. Skip TaskGarbageCollectionStage async execution.",
          event.getEventId(), event.getEventType(), event.getClusterName());
      return;
    }

    Map<String, Set<String>> expiredJobsMap =
        event.getAttribute(AttributeName.EXPIRED_JOBS_MAP.name());
    Set<String> toBeDeletedWorkflows =
        event.getAttribute(AttributeName.WORKFLOWS_TO_BE_DELETED.name());

    for (Map.Entry<String, Set<String>> entry : expiredJobsMap.entrySet()) {
      try {
        TaskUtil.purgeExpiredJobs(entry.getKey(), entry.getValue(), manager,
            _rebalanceScheduler);
      } catch (Exception e) {
        LOG.warn("Failed to purge job for workflow {}!", entry.getKey(), e);
      }
    }

    TaskUtil.workflowGarbageCollection(toBeDeletedWorkflows, manager);
  }

  private static void scheduleNextJobPurge(String workflow, long currentTime, long purgeInterval,
      RebalanceScheduler rebalanceScheduler, HelixManager manager) {
    long nextPurgeTime = currentTime + purgeInterval;
    long currentScheduledTime = rebalanceScheduler.getRebalanceTime(workflow);
    if (currentScheduledTime == -1 || currentScheduledTime > nextPurgeTime) {
      rebalanceScheduler.scheduleRebalance(manager, workflow, nextPurgeTime);
    }
  }
}
