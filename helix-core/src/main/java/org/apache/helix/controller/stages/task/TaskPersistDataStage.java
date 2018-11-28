package org.apache.helix.controller.stages.task;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskPersistDataStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(TaskPersistDataStage.class);

  @Override
  public void process(ClusterEvent event) {
    LOG.info("START TaskPersistDataStage.process()");
    long startTime = System.currentTimeMillis();

    // Persist partition assignment of resources.
    WorkflowControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    cache.getTaskDataCache().persistDataChanges(manager.getHelixDataAccessor());

    long endTime = System.currentTimeMillis();
    LOG.info(
        "END TaskPersistDataStage.process() for cluster " + cache.getClusterName() + " took " + (
            endTime - startTime) + " ms");
  }
}
