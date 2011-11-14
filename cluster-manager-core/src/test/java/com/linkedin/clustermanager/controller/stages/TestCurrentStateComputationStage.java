package com.linkedin.clustermanager.controller.stages;

import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;

public class TestCurrentStateComputationStage extends BaseStageTest
{

  @Test
  public void testEmptyCS()
  {
    Map<String, ResourceGroup> resourceGroupMap = getResourceGroupMap();
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(),
        resourceGroupMap);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output = event.getAttribute(AttributeName.CURRENT_STATE
        .toString());
    AssertJUnit.assertEquals(
        output.getCurrentStateMap("testResourceGroupName",
            new ResourceKey("testResourceGroupName_0")).size(), 0);
  }

  @Test
  public void testSimpleCS()
  {
    // setup resource group
    Map<String, ResourceGroup> resourceGroupMap = getResourceGroupMap();

    setupLiveInstances(5);

    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(),
        resourceGroupMap);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output1 = event.getAttribute(AttributeName.CURRENT_STATE
        .toString());
    AssertJUnit.assertEquals(
        output1.getCurrentStateMap("testResourceGroupName",
            new ResourceKey("testResourceGroupName_0")).size(), 0);

    // Add a state transition messages
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "msg1");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setStateUnitGroup("testResourceGroupName");
    message.setStateUnitKey("testResourceGroupName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");
    accessor.setProperty(PropertyType.MESSAGES, message.getRecord(),
        "localhost_" + 3, message.getId());
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output2 = event.getAttribute(AttributeName.CURRENT_STATE
        .toString());
    String pendingState = output2.getPendingState("testResourceGroupName",
        new ResourceKey("testResourceGroupName_1"), "localhost_3");
    AssertJUnit.assertEquals(pendingState, "SLAVE");

    ZNRecord record1 = new ZNRecord("testResourceGroupName");
    // Add a current state that matches sessionId and one that does not match
    CurrentState stateWithLiveSession = new CurrentState(record1);
    stateWithLiveSession.setSessionId("session_3");
    stateWithLiveSession.setStateModelDefRef("MasterSlave");
    stateWithLiveSession.setState("testResourceGroupName_1", "OFFLINE");
    ZNRecord record2 = new ZNRecord("testResourceGroupName");
    CurrentState stateWithDeadSession = new CurrentState(record2);
    stateWithDeadSession.setSessionId("session_dead");
    stateWithDeadSession.setStateModelDefRef("MasterSlave");
    stateWithDeadSession.setState("testResourceGroupName_1", "MASTER");

    accessor.setProperty(PropertyType.CURRENTSTATES,
        stateWithLiveSession.getRecord(), "localhost_3", "session_3",
        "testResourceGroupName");
    accessor.setProperty(PropertyType.CURRENTSTATES,
        stateWithDeadSession.getRecord(), "localhost_3", "session_dead",
        "testResourceGroupName");
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output3 = event.getAttribute(AttributeName.CURRENT_STATE
        .toString());
    String currentState = output3.getCurrentState("testResourceGroupName",
        new ResourceKey("testResourceGroupName_1"), "localhost_3");
    AssertJUnit.assertEquals(currentState, "OFFLINE");

  }

}
