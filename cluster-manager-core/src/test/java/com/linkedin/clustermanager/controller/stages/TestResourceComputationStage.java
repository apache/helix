package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.pipeline.StageContext;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

@Test (groups = {"unitTest"})
public class TestResourceComputationStage extends BaseStageTest
{
  /**
   * Case where we have one resource group in IdealState
   * 
   * @throws Exception
   */
  public void testSimple() throws Exception
  {
    int nodes = 5;
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++)
    {
      instances.add("localhost_" + i);
    }
    int partitions = 10;
    int replicas = 1;
    String resourceGroupName = "testResourceGroup";
    ZNRecord record = IdealStateCalculatorForStorageNode.calculateIdealState(
        instances, partitions, replicas, resourceGroupName, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("MasterSlave");
    manager.getDataAccessor().setProperty(PropertyType.IDEALSTATES,
        idealState.getRecord(), resourceGroupName);
    ResourceComputationStage stage = new ResourceComputationStage();
    runStage(event, stage);

    Map<String, ResourceGroup> resourceGroup = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    Assert.assertEquals(1, resourceGroup.size());

    Assert.assertEquals(resourceGroup.keySet().iterator().next(),
        resourceGroupName);
    Assert.assertEquals(resourceGroup.values().iterator().next()
        .getResourceGroupId(), resourceGroupName);
    Assert.assertEquals(resourceGroup.values().iterator().next()
        .getStateModelDefRef(), idealState.getStateModelDefRef());
    Assert.assertEquals(resourceGroup.values().iterator().next()
        .getResourceKeys().size(), partitions);
  }

  public void testMultipleResourceGroups() throws Exception
  {
    List<IdealState> idealStates = new ArrayList<IdealState>();
    String[] resourceGroups = new String[]
        { "testResourceGroup1", "testResourceGroup2" };
    setupIdealState(5, idealStates,resourceGroups);
    ResourceComputationStage stage = new ResourceComputationStage();
    runStage(event, stage);

    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    Assert.assertEquals(resourceGroups.length, resourceGroupMap.size());

    for (int i = 0; i < resourceGroups.length; i++)
    {
      String resourceGroupName = resourceGroups[i];
      IdealState idealState = idealStates.get(i);
      Assert.assertTrue(resourceGroupMap.containsKey(resourceGroupName));
      Assert.assertEquals(resourceGroupMap.get(resourceGroupName)
          .getResourceGroupId(), resourceGroupName);
      Assert.assertEquals(resourceGroupMap.get(resourceGroupName)
          .getStateModelDefRef(), idealState.getStateModelDefRef());
      Assert.assertEquals(resourceGroupMap.get(resourceGroupName)
          .getResourceKeys().size(), idealState.getNumPartitions());
    }
  }

 

  

  public void testMultipleResourceGroupsWithSomeDropped() throws Exception
  {
    int nodes = 5;
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++)
    {
      instances.add("localhost_" + i);
    }
    String[] resourceGroups = new String[]
    { "testResourceGroup1", "testResourceGroup2" };
    List<IdealState> idealStates = new ArrayList<IdealState>();
    for (int i = 0; i < resourceGroups.length; i++)
    {
      int partitions = 10;
      int replicas = 1;
      String resourceGroupName = resourceGroups[i];
      ZNRecord record = IdealStateCalculatorForStorageNode
          .calculateIdealState(instances, partitions, replicas,
              resourceGroupName, "MASTER", "SLAVE");
      IdealState idealState = new IdealState(record);
      idealState.setStateModelDefRef("MasterSlave");
      manager.getDataAccessor().setProperty(PropertyType.IDEALSTATES,
          idealState.getRecord(), resourceGroupName);
      idealStates.add(idealState);
    }
    // ADD A LIVE INSTANCE WITH A CURRENT STATE THAT CONTAINS RESOURCE WHICH NO
    // LONGER EXISTS IN IDEALSTATE
    String instanceName = "localhost_" + 3;
    ZNRecord liveInstanceRecord = new ZNRecord(instanceName);
    LiveInstance liveInstance = new LiveInstance(liveInstanceRecord);
    String sessionId = UUID.randomUUID().toString();
    liveInstance.setSessionId(sessionId);
    manager.getDataAccessor().setProperty(PropertyType.LIVEINSTANCES,
        liveInstanceRecord, instanceName);

    String oldResourceGroup = "testResourceOld";
    ZNRecord currentStateRecord = new ZNRecord(oldResourceGroup);
    CurrentState currentState = new CurrentState(currentStateRecord);
    currentState.setState("testResourceOld_0", "OFFLINE");
    currentState.setState("testResourceOld_1", "SLAVE");
    currentState.setState("testResourceOld_2", "MASTER");
    currentStateRecord.setSimpleField(
        Message.Attributes.STATE_MODEL_DEF.toString(), "MasterSlave");
    manager.getDataAccessor().setProperty(PropertyType.CURRENTSTATES,
        currentStateRecord, instanceName, sessionId, oldResourceGroup);

    ResourceComputationStage stage = new ResourceComputationStage();
    runStage(event, stage);

    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    // +1 because it will have one for current state
    Assert.assertEquals(resourceGroups.length + 1, resourceGroupMap.size());

    for (int i = 0; i < resourceGroups.length; i++)
    {
      String resourceGroupName = resourceGroups[i];
      IdealState idealState = idealStates.get(i);
      Assert.assertTrue(resourceGroupMap.containsKey(resourceGroupName));
      Assert.assertEquals(resourceGroupMap.get(resourceGroupName)
          .getResourceGroupId(), resourceGroupName);
      Assert.assertEquals(resourceGroupMap.get(resourceGroupName)
          .getStateModelDefRef(), idealState.getStateModelDefRef());
      Assert.assertEquals(resourceGroupMap.get(resourceGroupName)
          .getResourceKeys().size(), idealState.getNumPartitions());
    }
    // Test the data derived from CurrentState
    Assert.assertTrue(resourceGroupMap.containsKey(oldResourceGroup));
    Assert.assertEquals(resourceGroupMap.get(oldResourceGroup)
        .getResourceGroupId(), oldResourceGroup);
    Assert.assertEquals(resourceGroupMap.get(oldResourceGroup)
        .getStateModelDefRef(), currentState.getStateModelDefRef());
    Assert
        .assertEquals(resourceGroupMap.get(oldResourceGroup).getResourceKeys()
            .size(), currentState.getResourceKeyStateMap().size());
    Assert.assertNotNull(resourceGroupMap.get(oldResourceGroup).getResourceKey("testResourceOld_0"));
    Assert.assertNotNull(resourceGroupMap.get(oldResourceGroup).getResourceKey("testResourceOld_1"));
    Assert.assertNotNull(resourceGroupMap.get(oldResourceGroup).getResourceKey("testResourceOld_2"));
    
  }
  
  public void testNull()
  {
    ClusterEvent event = new ClusterEvent("sampleEvent");
    ResourceComputationStage stage = new ResourceComputationStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    boolean exceptionCaught = false;
    try
    {
      stage.process(event);
    } catch (Exception e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
    stage.postProcess();
  }
  
  /*
  public void testEmptyCluster()
  {
    ClusterEvent event = new ClusterEvent("sampleEvent");
    ClusterManager manager = new Mocks.MockManager();
    event.addAttribute("clustermanager", manager);
    ResourceComputationStage stage = new ResourceComputationStage();
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    boolean exceptionCaught = false;
    try
    {
      stage.process(event);
    } catch (Exception e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
    stage.postProcess();
  }
  */
}
