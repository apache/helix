package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.PersistAssignmentStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPersistAssignmentStage extends ZkStandAloneCMTestBase {
  ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);

  /**
   * Case where we have one resource in IdealState
   * @throws Exception
   */
  @Test
  public void testSimple() throws Exception {
    int nodes = 2;
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++) {
      instances.add("localhost_" + i);
    }
    int partitions = 10;
    int replicas = 1;
    String resourceName = "testResource";
    ZNRecord record =
        DefaultIdealStateCalculator.calculateIdealState(instances, partitions, replicas, resourceName, "ONLINE",
            "OFFLINE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("OnlineOffline");

    // Read and load current state into event
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    runStage(event, new ReadClusterDataStage());
    runStage(event, new ResourceComputationStage());

    // Ensure persist best possible assignment is true
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
    clusterConfig.setPersistBestPossibleAssignment(true);
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    cache.setClusterConfig(clusterConfig);

    // 1. Change best possible state (simulate a new rebalancer run)
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String partition : idealState.getPartitionSet()) {
      bestPossibleStateOutput.setState(resourceName, new Partition(partition), "localhost_3", "OFFLINE");
    }
    // 2. At the same time, set DelayRebalanceEnabled = true (simulate a Admin operation at the same time)
    idealState.setDelayRebalanceEnabled(true);
    accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);

    // Persist new assignment
    PersistAssignmentStage stage = new PersistAssignmentStage();
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    runStage(event, stage);

    IdealState newIdealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    // 1. New assignment should be set
    Assert.assertEquals(newIdealState.getPartitionSet().size(), idealState.getPartitionSet().size());
    for (String partition : idealState.getPartitionSet()) {
      Map<String, String> assignment = newIdealState.getInstanceStateMap(partition);
      Assert.assertNotNull(assignment);
      Assert.assertEquals(assignment.size(),1);
      Assert.assertTrue(assignment.containsKey("localhost_3") && assignment.get("localhost_3").equals("OFFLINE"));
    }
    // 2. Admin config should be set
    Assert.assertTrue(newIdealState.isDelayRebalanceEnabled());
  }

  private void runStage(ClusterEvent event, Stage stage) {
    event.addAttribute(AttributeName.helixmanager.name(), _manager);
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try {
      stage.process(event);
    } catch (Exception e) {
      e.printStackTrace();
    }
    stage.postProcess();
  }
}
