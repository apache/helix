package org.apache.helix.controller.stages;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskGarbageCollectionStage extends AbstractAsyncBaseStage {
  private static RebalanceScheduler _rebalanceScheduler = new RebalanceScheduler();

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.TaskJobPurgeWorker;
  }

  @Override
  public void execute(ClusterEvent event) throws Exception {
    ClusterDataCache clusterDataCache = event.getAttribute(AttributeName.ClusterDataCache.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    for (WorkflowConfig workflowConfig : clusterDataCache.getWorkflowConfigMap().values()) {
      // clean up the expired jobs if it is a queue.
      if (!workflowConfig.isTerminable() || workflowConfig.isJobQueue()) {
        TaskUtil.purgeExpiredJobs(workflowConfig.getWorkflowId(), workflowConfig,
            clusterDataCache.getWorkflowContext(workflowConfig.getWorkflowId()), manager,
            _rebalanceScheduler);
      }
    }
  }
}
