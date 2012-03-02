package com.linkedin.helix.controller.stages;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.controller.pipeline.Pipeline;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Partition;

public class TestRebalancePipeline extends ZkUnitTestBase
{
  private static final Logger LOG =
      Logger.getLogger(TestRebalancePipeline.class.getName());
  final String _className = getShortClassName();
  HelixManager _manager;
  DataAccessor _accessor;
  ClusterEvent _event;

  @Test
  public void testDuplicateMsg()
  {
    String clusterName = "CLUSTER_" + _className + "_dup";
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    _accessor = new ZKDataAccessor(clusterName, _gZkClient);
    _manager = new DummyClusterManager(clusterName, _accessor);
    _event = new ClusterEvent("testEvent");
    _event.addAttribute("helixmanager", _manager);

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] { resourceName };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] { 0, 1 }, resourceGroups, 1, 2);
    setupLiveInstances(clusterName, new int[] { 0, 1 });
    setupStateModel(clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node0 currentState to OFFLINE and node1 currentState to OFFLINE
    setCurrentState("localhost_0",
                    resourceName,
                    resourceName + "_0",
                    "session_0",
                    "OFFLINE");
    setCurrentState("localhost_1",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "SLAVE");

    runPipeline(_event, dataRefresh);
    runPipeline(_event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        _event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName
            + "_0"));
    Assert.assertEquals(messages.size(),
                        1,
                        "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not send S->M until removal is done
    setCurrentState("localhost_0",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "SLAVE");

    runPipeline(_event, dataRefresh);
    runPipeline(_event, rebalancePipeline);
    msgSelOutput = _event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(),
                        0,
                        "Should NOT output 1 message: SLAVE-MASTER for node1");

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  protected void setCurrentState(String instance,
                                 String resourceGroupName,
                                 String resourceKey,
                                 String sessionId,
                                 String state)
  {
    CurrentState curState = new CurrentState(resourceGroupName);
    curState.setState(resourceKey, state);
    curState.setSessionId(sessionId);
    curState.setStateModelDefRef("MasterSlave");
    _accessor.setProperty(PropertyType.CURRENTSTATES,
                          curState,
                          instance,
                          sessionId,
                          resourceGroupName);
  }
}